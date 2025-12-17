package vn.bnh;

import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataValidator {

    private static final Logger LOGGER = Logger.getLogger(DataValidator.class.getName());

    private static final class Env {
        static String get(String key) {
            String v = System.getenv(key);
            if (v == null || v.isEmpty()) throw new IllegalStateException("Missing env: " + key);
            return v;
        }

        static int getInt(String key) {
            return Integer.parseInt(get(key));
        }

        static long getLong(String key) {
            return Long.parseLong(get(key));
        }
    }

    private static final class Pools {
        static PoolDataSource app() throws SQLException {
            PoolDataSource p = PoolDataSourceFactory.getPoolDataSource();
            p.setConnectionFactoryClassName(OracleDataSource.class.getName());
            p.setURL("jdbc:oracle:thin:@//" + Env.get("RAC_HOST") + ":" + Env.get("RAC_PORT") + "/" + Env.get("RAC_APP_SERVICE"));
            p.setUser(Env.get("RAC_USERNAME"));
            p.setPassword(Env.get("RAC_PASSWORD"));
            p.setInitialPoolSize(Env.getInt("APP_INITIAL_POOL_SIZE"));
            p.setMinPoolSize(Env.getInt("APP_MIN_POOL_SIZE"));
            p.setMaxPoolSize(Env.getInt("APP_MAX_POOL_SIZE"));
            return p;
        }

        static void closeQuietly(Object pool) {
            if (pool == null) return;
            try {
                pool.getClass().getMethod("close").invoke(pool);
            } catch (Throwable ignored) {
            }
        }
    }

    private static final class Exec {
        static ExecutorService fixed(int threads, String prefix) {
            AtomicInteger idx = new AtomicInteger(1);
            ThreadFactory tf = r -> {
                Thread t = new Thread(r);
                t.setName(prefix + idx.getAndIncrement());
                return t;
            };
            return Executors.newFixedThreadPool(threads, tf);
        }
    }

    private static final class Baseline {
        final String[] idNos;
        final long[] expectedCnts;

        Baseline(String[] idNos, long[] expectedCnts) {
            this.idNos = idNos;
            this.expectedCnts = expectedCnts;
        }

        int randomIndex() {
            return ThreadLocalRandom.current().nextInt(idNos.length);
        }

        String idNo(int idx) {
            return idNos[idx];
        }

        long expected(int idx) {
            return expectedCnts[idx];
        }
    }

    private static final class Counters {
        final long max;
        final AtomicLong done = new AtomicLong(0);
        final AtomicLong errors = new AtomicLong(0);
        final Object lock = new Object();

        Counters(long max) {
            this.max = max;
        }

        boolean reached() {
            return done.get() >= max;
        }

        boolean record(boolean ok) {
            synchronized (lock) {
                if (done.get() >= max) return false;
                done.incrementAndGet();
                if (!ok) errors.incrementAndGet();
                return done.get() < max;
            }
        }
    }

    private static final class MismatchStat {
        final AtomicLong count = new AtomicLong(0);
        final AtomicLong sampleExpected = new AtomicLong(Long.MIN_VALUE);
        final AtomicLong sampleActual = new AtomicLong(Long.MIN_VALUE);

        void record(long expected, long actual) {
            count.incrementAndGet();
            sampleExpected.compareAndSet(Long.MIN_VALUE, expected);
            sampleActual.compareAndSet(Long.MIN_VALUE, actual);
        }
    }

    private static final String BASELINE_SQL = "SELECT ID_NO, COUNT(*) AS CNT " + "FROM CUST_IDENTITY " + "GROUP BY ID_NO";

    private static final String VALIDATE_SQL = "SELECT COUNT(*) " + "FROM SUBSCRIBER SUB " + "WHERE SUB.CUST_ID IN ( " + "    SELECT DISTINCT CI.CUST_ID " + "    FROM CUST_IDENTITY CI, CUSTOMER CUS " + "    WHERE CI.CUST_ID = CUS.CUST_ID " + "      AND CI.ID_NO   = ? " + "      AND CI.STATUS  = '1' " + "      AND CUS.STATUS = '1' " + ")";

    private static final ConcurrentHashMap<String, MismatchStat> mismatches = new ConcurrentHashMap<>();

    private static Baseline loadBaseline(PoolDataSource pool) throws SQLException {
        List<String> ids = new ArrayList<>();
        List<Long> cnts = new ArrayList<>();

        try (Connection conn = pool.getConnection(); PreparedStatement ps = conn.prepareStatement(BASELINE_SQL); ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String idNo = rs.getString(1);
                long cnt = rs.getLong(2);
                if (idNo == null) continue;
                ids.add(idNo);
                cnts.add(cnt);
            }
        }

        if (ids.isEmpty()) throw new IllegalStateException("Baseline is empty (no ID_NO found).");

        String[] idNos = ids.toArray(new String[0]);
        long[] expected = new long[cnts.size()];
        for (int i = 0; i < cnts.size(); i++) expected[i] = cnts.get(i);

        LOGGER.info("Baseline loaded: distinct_id_no=" + idNos.length);
        return new Baseline(idNos, expected);
    }

    private static long queryCount(PoolDataSource pool, String idNo) throws SQLException {
        try (Connection conn = pool.getConnection(); PreparedStatement ps = conn.prepareStatement(VALIDATE_SQL)) {

            ps.setString(1, idNo);

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return 0L;
                return rs.getLong(1);
            }
        }
    }

    private static boolean doOne(PoolDataSource pool, Baseline baseline, Counters counters) throws SQLException {
        if (counters.reached()) return false;

        int idx = baseline.randomIndex();
        String idNo = baseline.idNo(idx);
        long expected = baseline.expected(idx);

        long actual = queryCount(pool, idNo);
        boolean ok = (actual == expected);

        if (!ok) mismatches.computeIfAbsent(idNo, k -> new MismatchStat()).record(expected, actual);

        return counters.record(ok);
    }

    private static void printMismatchReport(long total, long errors) {
        if (errors == 0) {
            LOGGER.info("Mismatch report: no mismatches.");
            return;
        }

        List<Map.Entry<String, MismatchStat>> rows = new ArrayList<>(mismatches.entrySet());
        rows.sort(Comparator.comparingLong((Map.Entry<String, MismatchStat> e) -> e.getValue().count.get()).reversed());

        LOGGER.info("Mismatch report (unique_id_no_with_mismatch=" + rows.size() + ", total_mismatches=" + errors + ", total_queries=" + total + "):");

        int limit = Math.min(20, rows.size());
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, MismatchStat> e = rows.get(i);
            MismatchStat st = e.getValue();
            LOGGER.info("  ID_NO=" + e.getKey() + " MISMATCH_COUNT=" + st.count.get() + " SAMPLE_EXPECTED=" + st.sampleExpected.get() + " SAMPLE_ACTUAL=" + st.sampleActual.get());
        }
    }

    public static void run() {
        PoolDataSource pool = null;

        try {
            int threads = Env.getInt("THREAD_COUNT");
            long maxQueries = Env.getLong("MAX_ID");

            pool = Pools.app();
            final PoolDataSource appPool = pool;

            Baseline baseline = loadBaseline(appPool);
            Counters counters = new Counters(maxQueries);

            LOGGER.info("DataValidator STARTED with threads=" + threads + ", MAX_ID=" + maxQueries);

            long startNs = System.nanoTime();

            ExecutorService executor = Exec.fixed(threads, "validator-");
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        while (doOne(appPool, baseline, counters)) {
                        }
                    } catch (SQLException e) {
                        LOGGER.log(Level.SEVERE, "[" + Thread.currentThread().getName() + "] SQL ERROR (STOP)", e);
                        System.exit(1);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "[" + Thread.currentThread().getName() + "] UNEXPECTED ERROR (STOP)", e);
                        System.exit(1);
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

            double elapsedSec = (System.nanoTime() - startNs) / 1_000_000_000.0;

            long total = counters.done.get();
            long errors = counters.errors.get();

            double qps = elapsedSec <= 0.0 ? 0.0 : (total / elapsedSec);
            double errorRate = total == 0 ? 0.0 : (errors * 1.0 / total);

            LOGGER.info("DataValidator FINISHED, TOTAL=" + total + ", ERRORS=" + errors + ", ERROR_RATE=" + errorRate + ", ELAPSED_SEC=" + elapsedSec + ", QPS=" + qps);

            printMismatchReport(total, errors);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL", e);
            System.exit(1);
        } finally {
            Pools.closeQuietly(pool);
        }
    }
}
