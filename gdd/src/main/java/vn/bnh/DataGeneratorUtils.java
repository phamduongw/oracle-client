package vn.bnh;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

final class DataGeneratorUtils {

    static final String SQL_INSERT_CUSTOMER = "INSERT INTO CUSTOMER_T1 (CUST_ID, CUST_TYPE, NAME, BIRTH_DATE, SEX, NATIONALITY, VIP, STATUS, AREA_CODE, PROVINCE, DISTRICT, PRECINCT, STREET_BLOCK, STREET, STREET_NAME, STREET_BLOCK_NAME, HOME, ADDRESS, CREATE_USER, CREATE_DATETIME, UPDATE_DATETIME, UPDATE_USER, DESCRIPTION, UPDATE_NUMBER2, LAST_MODIFIED, PARENT_ID) " + "VALUES (?, 'VIE', 'TEST DIA BAN', TO_DATE('2025-06-05 09:22:51', 'YYYY-MM-DD HH24:MI:SS'), 'M', 'Việt Nam', NULL, '1', 'HNI0400060', 'HNI', NULL, '040', '0060', NULL, NULL, 'Tổ 1-Bồ Đề', NULL, 'Tổ 1-Bồ Đề, Phường Bồ Đề Thành phố Hà Nội', 'VTT1', TO_DATE('2025-06-12 14:02:28', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('2025-10-14 18:17:39', 'YYYY-MM-DD HH24:MI:SS'), 'VTT1', NULL, NULL, NULL, NULL)";

    static final String SQL_CALL_INSERT_CUSTOMER_BATCH = "{ call INSERT_CUSTOMER_BATCH(?) }";
    static final String ORACLE_TYPE_CUST_ID_TABLE = "T_CUST_ID_TAB";

    private static final Logger LOGGER = Logger.getLogger(DataGeneratorUtils.class.getName());

    private static final AtomicLong ORA_45582_TOTAL = new AtomicLong(0);
    private static final AtomicLong ORA_05086_TOTAL = new AtomicLong(0);

    private static final AtomicLong LAST_ROUTING_REFRESH_MS = new AtomicLong(0);
    private static final long ROUTING_REFRESH_COOLDOWN_MS = 5_000;

    private DataGeneratorUtils() {
    }

    static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isEmpty()) throw new IllegalStateException("Missing env: " + key);
        return value;
    }

    static PoolDataSource createPool(String jdbcUrlEnvKey, int workerThreads) throws Exception {
        int shardCount = Integer.parseInt(requireEnv("SHARDS"));
        int poolSize = Math.multiplyExact(workerThreads, shardCount);

        PoolDataSource pool = PoolDataSourceFactory.getPoolDataSource();
        pool.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        pool.setFastConnectionFailoverEnabled(true);
        pool.setConnectionPoolName("UCP_" + jdbcUrlEnvKey);
        pool.setURL(requireEnv(jdbcUrlEnvKey));
        pool.setUser(requireEnv("GDD_USERNAME"));
        pool.setPassword(requireEnv("GDD_PASSWORD"));
        pool.setInitialPoolSize(poolSize);
        pool.setMinPoolSize(poolSize);
        pool.setMaxPoolSize(poolSize);
        return pool;
    }

    static Connection openConnectionByShardKey(PoolDataSource gdsPool, BigDecimal shardKey) throws Exception {
        return gdsPool.createConnectionBuilder().shardingKey(buildNumberShardingKey(gdsPool, shardKey)).build();
    }

    static String resolveShardNameByShardKey(PoolDataSource gdsPool, BigDecimal shardKey) throws Exception {
        int maxAttempts = 3;
        long backoffMs = 100;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try (Connection conn = openConnectionByShardKey(gdsPool, shardKey); PreparedStatement ps = conn.prepareStatement("SELECT name FROM v$database"); ResultSet rs = ps.executeQuery()) {

                rs.next();
                return rs.getString(1);
            } catch (SQLException e) {
                if (isOra45582(e)) {
                    recordOra45582(gdsPool, "resolve_shard_name", shardKey, e);
                    if (attempt == maxAttempts) throw e;
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 1_000);
                    continue;
                }
                throw e;
            }
        }

        throw new IllegalStateException("unreachable");
    }

    static void insertCustomerById(PoolDataSource gdsPool, BigDecimal customerId) throws Exception {
        try (Connection conn = openConnectionByShardKey(gdsPool, customerId); PreparedStatement ps = conn.prepareStatement(SQL_INSERT_CUSTOMER)) {

            conn.setAutoCommit(false);
            ps.setBigDecimal(1, customerId);
            ps.executeUpdate();
            conn.commit();
        }
    }

    static void callInsertCustomerBatchWithRetry(PoolDataSource gdsPool, BigDecimal routeKey, BigDecimal[] customerIds) throws Exception {

        int maxAttempts = 3;
        long backoffMs = 200;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                callInsertCustomerBatch(gdsPool, routeKey, customerIds);
                return;
            } catch (SQLException e) {
                if (isOra45582(e)) {
                    recordOra45582(gdsPool, "call_insert_customer_batch", routeKey, e);
                    if (attempt == maxAttempts) throw e;
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5_000);
                    continue;
                }
                if (isOra05086(e)) {
                    recordOra05086("call_insert_customer_batch", routeKey, e);
                    if (attempt == maxAttempts) throw e;
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5_000);
                    continue;
                }
                throw e;
            }
        }

        throw new IllegalStateException("unreachable");
    }

    static void runForDuration(int workerThreads, long durationSeconds, Runnable task) throws Exception {
        long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
        ExecutorService executor = Executors.newFixedThreadPool(workerThreads);

        for (int i = 0; i < workerThreads; i++) {
            executor.submit(() -> {
                while (System.nanoTime() < deadlineNs) {
                    task.run();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    static void logStopAndExit(Logger logger, String mode, Throwable error) {
        logger.log(Level.SEVERE, "STOP mode=" + mode, error);
        System.exit(1);
    }

    static void logFatalAndExit(Logger logger, String mode, Throwable error) {
        logger.log(Level.SEVERE, "FATAL mode=" + mode, error);
        System.exit(1);
    }

    private static OracleShardingKey buildNumberShardingKey(PoolDataSource pool, BigDecimal shardKey) throws Exception {
        return pool.createShardingKeyBuilder().subkey(shardKey, OracleType.NUMBER).build();
    }

    private static void callInsertCustomerBatch(PoolDataSource gdsPool, BigDecimal routeKey, BigDecimal[] customerIds) throws Exception {

        try (Connection conn = openConnectionByShardKey(gdsPool, routeKey); CallableStatement cs = conn.prepareCall(SQL_CALL_INSERT_CUSTOMER_BATCH)) {

            conn.setAutoCommit(false);

            OracleConnection oracleConn = conn.unwrap(OracleConnection.class);
            Array oracleArray = oracleConn.createOracleArray(ORACLE_TYPE_CUST_ID_TABLE, customerIds);

            cs.setArray(1, oracleArray);
            cs.execute();

            conn.commit();
        }
    }

    private static boolean isOra45582(SQLException e) {
        if (e.getErrorCode() == 45582) return true;
        String msg = e.getMessage();
        return msg != null && msg.contains("ORA-45582");
    }

    private static boolean isOra05086(SQLException e) {
        if (e.getErrorCode() == 5086) return true;
        String msg = e.getMessage();
        return msg != null && msg.contains("ORA-05086");
    }

    private static void recordOra45582(PoolDataSource pool, String phase, Object key, SQLException cause) {
        long total = ORA_45582_TOTAL.incrementAndGet();
        LOGGER.log(Level.WARNING, "ORA45582 total={0} phase={1} pool={2} key={3} msg={4}", new Object[]{total, phase, safePoolName(pool), String.valueOf(key), cause.getMessage()});
        refreshRoutingWithCooldown(pool, phase);
    }

    private static void recordOra05086(String phase, Object key, SQLException cause) {
        long total = ORA_05086_TOTAL.incrementAndGet();
        LOGGER.log(Level.WARNING, "ORA05086 total={0} phase={1} key={2} msg={3}", new Object[]{total, phase, String.valueOf(key), cause.getMessage()});
    }

    private static void refreshRoutingWithCooldown(PoolDataSource pool, String phase) {
        long now = System.currentTimeMillis();
        long last = LAST_ROUTING_REFRESH_MS.get();

        if (now - last < ROUTING_REFRESH_COOLDOWN_MS) {
            LOGGER.log(Level.WARNING, "ROUTING_REFRESH_SKIPPED phase={0} pool={1} since_ms={2}", new Object[]{phase, safePoolName(pool), (now - last)});
            return;
        }

        if (!LAST_ROUTING_REFRESH_MS.compareAndSet(last, now)) return;

        try {
            UniversalConnectionPoolManager mgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
            mgr.refreshConnectionPool(pool.getConnectionPoolName());
            LOGGER.log(Level.WARNING, "ROUTING_REFRESH_OK phase={0} pool={1}", new Object[]{phase, safePoolName(pool)});
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "ROUTING_REFRESH_FAIL phase=" + phase + " pool=" + safePoolName(pool), ex);
        }
    }

    private static String safePoolName(PoolDataSource pool) {
        try {
            return pool.getConnectionPoolName();
        } catch (Exception e) {
            return "UNKNOWN_POOL";
        }
    }
}
