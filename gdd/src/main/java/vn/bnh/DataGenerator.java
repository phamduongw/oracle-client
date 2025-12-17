package vn.bnh;

import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataGenerator {

    private static final Logger LOGGER = Logger.getLogger(DataGenerator.class.getName());

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

    private static final class Sql {
        static String load(String path) throws Exception {
            try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
                if (is == null) throw new IllegalStateException("Missing SQL file: " + path);
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
    }

    private static final class Pools {
        static PoolDataSource app() throws SQLException {
            PoolDataSource p = PoolDataSourceFactory.getPoolDataSource();
            p.setConnectionFactoryClassName(OracleDataSource.class.getName());
            p.setURL("jdbc:oracle:thin:@//" + Env.get("GDD_HOST") + ":" + Env.get("GDD_PORT") + "/" + Env.get("GDD_APP_SERVICE"));
            p.setUser(Env.get("GDD_USERNAME"));
            p.setPassword(Env.get("GDD_PASSWORD"));
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

    private static String customerSql;
    private static String custIdentitySql;
    private static String accountSql;
    private static String subscriberSql;

    private static long maxTransactions;
    private static final AtomicLong success = new AtomicLong(0);
    private static final Object lock = new Object();

    private static SnowflakeGenerator generator(int threadIndex1Based) {
        return new SnowflakeGenerator(threadIndex1Based - 1L);
    }

    private static OracleShardingKey shardingKey(PoolDataSource pool, long custId) throws SQLException {
        return pool.createShardingKeyBuilder().subkey(custId, JDBCType.NUMERIC).build();
    }

    private static boolean executeTransaction(PoolDataSource pool, SnowflakeGenerator gen) {
        if (success.get() >= maxTransactions) return false;

        long custId = gen.generateId();
        long custIdentityId = gen.generateId();
        long accountId = gen.generateId();
        long subId = gen.generateId();
        long contractId = gen.generateId();

        try (Connection conn = pool.createConnectionBuilder().shardingKey(shardingKey(pool, custId)).build()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(customerSql)) {
                ps.setLong(1, custId);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(custIdentitySql)) {
                ps.setLong(1, custIdentityId);
                ps.setLong(2, custId);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(accountSql)) {
                ps.setLong(1, accountId);
                ps.setLong(2, custId);
                ps.setLong(3, subId);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(subscriberSql)) {
                ps.setLong(1, subId);
                ps.setLong(2, contractId);
                ps.setLong(3, custId);
                ps.setLong(4, accountId);
                ps.executeUpdate();
            }

            synchronized (lock) {
                if (success.get() >= maxTransactions) {
                    conn.rollback();
                    return false;
                }
                conn.commit();
                long done = success.incrementAndGet();
                LOGGER.info("[" + Thread.currentThread().getName() + "] COMMIT CUST_ID=" + custId + " SUB_ID=" + subId + " TOTAL=" + done);
                return done < maxTransactions;
            }
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "[" + Thread.currentThread().getName() + "] SQL ERROR (STOP) CUST_ID=" + custId + " SUB_ID=" + subId, e);
            System.exit(1);
            return false;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[" + Thread.currentThread().getName() + "] UNEXPECTED ERROR (STOP) CUST_ID=" + custId + " SUB_ID=" + subId, e);
            System.exit(1);
            return false;
        }
    }

    public static void run() {
        PoolDataSource pool = null;
        try {
            customerSql = Sql.load("sql/insert_customer.sql");
            custIdentitySql = Sql.load("sql/insert_cust_identity.sql");
            accountSql = Sql.load("sql/insert_account.sql");
            subscriberSql = Sql.load("sql/insert_subscriber.sql");

            int threads = Env.getInt("THREAD_COUNT");
            maxTransactions = Env.getLong("MAX_ID");

            pool = Pools.app();
            final PoolDataSource appPool = pool;

            LOGGER.info("DataGenerator STARTED with threads=" + threads + ", MAX_ID=" + maxTransactions);

            ExecutorService executor = Exec.fixed(threads, "data-gen-");
            for (int i = 1; i <= threads; i++) {
                int threadIndex = i;
                executor.submit(() -> {
                    SnowflakeGenerator gen = generator(threadIndex);
                    while (executeTransaction(appPool, gen)) {
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

            LOGGER.info("DataGenerator FINISHED, TOTAL_SUCCESS=" + success.get());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL", e);
            System.exit(1);
        } finally {
            Pools.closeQuietly(pool);
        }
    }
}
