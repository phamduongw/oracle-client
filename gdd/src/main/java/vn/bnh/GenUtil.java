package vn.bnh;

import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

final class GenUtil {

    private static final Logger LOGGER = Logger.getLogger(GenUtil.class.getName());

    static final String INSERT_SQL_UUID = "INSERT INTO DATA_SUBCRIBER_U (DATA_SUBCRIBER_ID, MSISDN, SUB_ID, SER_TYPE, EFFECT_DATE, EXPIRED_DATE, SCAN_TIME, " + "SERVICE_TYPE, DESCRIPTION, EXTEND_TYPE, PROMOTION_CODE, PROMOTION_START_TIME, PROMOTION_END_TIME, NODE_NAME, CLUSTER_NAME, " + "SUB_ID_BCCSVAS2, PREFIX_MSISDN, EXTEND_PROMOTION_CODE, NUM_ON_FLEXIBLE) " + "VALUES (?, '84397549982', '30146217018', 'ST5K', SYSDATE, TRUNC(SYSDATE)+1, TRUNC(SYSDATE)+1, " + "'1', 'Test', '3', '', '', '', 'extend_fb_ht1_node1', 'EXTEND_FB_HT1', '30146217018', '84397', '', 0)";

    static final String INSERT_SQL_SEQ = "INSERT INTO DATA_SUBCRIBER_S (DATA_SUBCRIBER_ID, MSISDN, SUB_ID, SER_TYPE, EFFECT_DATE, EXPIRED_DATE, SCAN_TIME, " + "SERVICE_TYPE, DESCRIPTION, EXTEND_TYPE, PROMOTION_CODE, PROMOTION_START_TIME, PROMOTION_END_TIME, NODE_NAME, CLUSTER_NAME, " + "SUB_ID_BCCSVAS2, PREFIX_MSISDN, EXTEND_PROMOTION_CODE, NUM_ON_FLEXIBLE) " + "VALUES (?, '84397549982', '30146217018', 'ST5K', SYSDATE, TRUNC(SYSDATE)+1, TRUNC(SYSDATE)+1, " + "'1', 'Test', '3', '', '', '', 'extend_fb_ht1_node1', 'EXTEND_FB_HT1', '30146217018', '84397', '', 0)";

    static final AtomicLong OK = new AtomicLong();
    static final AtomicLong ERR = new AtomicLong();

    private GenUtil() {
    }

    static String env(String k) {
        String v = System.getenv(k);
        if (v == null || v.isEmpty()) throw new IllegalStateException("Missing env: " + k);
        return v;
    }

    static PoolDataSource pool(String urlEnvKey, int threads) throws Exception {
        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        pds.setConnectionPoolName("POOL_" + urlEnvKey);
//        pds.setFastConnectionFailoverEnabled(true);
        pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        pds.setURL(env(urlEnvKey));
        pds.setUser(env("GDD_USERNAME"));
        pds.setPassword(env("GDD_PASSWORD"));
        pds.setInitialPoolSize(threads);
        pds.setMinPoolSize(threads);
        pds.setMaxPoolSize(threads);
        return pds;
    }

    static OracleShardingKey shardingKey(PoolDataSource pds, Object id, OracleType type) throws Exception {
        return pds.createShardingKeyBuilder().subkey(id, type).build();
    }

    static void insertUuid(PoolDataSource gds, byte[] id) throws Exception {
        try (Connection conn = gds.createConnectionBuilder().shardingKey(shardingKey(gds, id, OracleType.RAW)).build(); PreparedStatement ps = conn.prepareStatement(INSERT_SQL_UUID)) {
            conn.setAutoCommit(false);
            ps.setBytes(1, id);
            ps.executeUpdate();
            conn.commit();
            OK.incrementAndGet();
        }
    }

    static void insertSeq(PoolDataSource gds, BigDecimal id) throws Exception {
        try (Connection conn = gds.createConnectionBuilder().shardingKey(shardingKey(gds, id, OracleType.NUMBER)).build(); PreparedStatement ps = conn.prepareStatement(INSERT_SQL_SEQ)) {
            conn.setAutoCommit(false);
            ps.setBigDecimal(1, id);
            ps.executeUpdate();
            conn.commit();
            OK.incrementAndGet();
        }
    }

    static void runLoop(int threads, long durationSeconds, Runnable once, String mode) throws Exception {
        long startNs = System.nanoTime();
        long deadline = startNs + TimeUnit.SECONDS.toNanos(durationSeconds);

        ExecutorService workers = Executors.newFixedThreadPool(threads);
        ScheduledExecutorService hb = Executors.newSingleThreadScheduledExecutor();

        AtomicLong lastOk = new AtomicLong();
        AtomicLong lastNs = new AtomicLong(System.nanoTime());

        hb.scheduleAtFixedRate(() -> {
            long now = System.nanoTime();
            long ok = OK.get();
            long err = ERR.get();
            long prevOk = lastOk.getAndSet(ok);
            long prevNs = lastNs.getAndSet(now);
            long dt = now - prevNs;
            long dOk = ok - prevOk;
            long tps = dt > 0 ? (dOk * 1_000_000_000L) / dt : 0L;
            LOGGER.log(Level.INFO, "HEARTBEAT mode={0} ok={1} err={2} tps={3}", new Object[]{mode, ok, err, tps});
        }, 5, 5, TimeUnit.SECONDS);

        for (int i = 0; i < threads; i++)
            workers.submit(() -> {
                while (System.nanoTime() < deadline) once.run();
            });

        workers.shutdown();
        workers.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        hb.shutdownNow();
        hb.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        long endNs = System.nanoTime();
        long ok = OK.get();
        long err = ERR.get();
        long tps = (endNs - startNs) > 0 ? (ok * 1_000_000_000L) / (endNs - startNs) : 0L;
        LOGGER.log(Level.INFO, "SUMMARY mode={0} duration_s={1} ok={2} err={3} avg_tps={4}", new Object[]{mode, (endNs - startNs) / 1_000_000_000L, ok, err, tps});
    }
}
