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

final class DataUtil {

    static final String INSERT_SQL_UUID = "INSERT INTO DATA_SUBCRIBER_U (DATA_SUBCRIBER_ID, MSISDN, SUB_ID, SER_TYPE, EFFECT_DATE, EXPIRED_DATE, SCAN_TIME, SERVICE_TYPE, DESCRIPTION, EXTEND_TYPE, PROMOTION_CODE, PROMOTION_START_TIME, PROMOTION_END_TIME, NODE_NAME, CLUSTER_NAME, SUB_ID_BCCSVAS2, PREFIX_MSISDN, EXTEND_PROMOTION_CODE, NUM_ON_FLEXIBLE) VALUES (?, '84397549982', '30146217018', 'ST5K', SYSDATE, TRUNC(SYSDATE)+1, TRUNC(SYSDATE)+1, '1', 'Test', '3', '', '', '', 'extend_fb_ht1_node1', 'EXTEND_FB_HT1', '30146217018', '84397', '', 0)";
    static final String INSERT_SQL_CUSTOMER = "INSERT INTO CUSTOMER_T1 (CUST_ID, CUST_TYPE, NAME, BIRTH_DATE, SEX, NATIONALITY, VIP, STATUS, AREA_CODE, PROVINCE, DISTRICT, PRECINCT, STREET_BLOCK, STREET, STREET_NAME, STREET_BLOCK_NAME, HOME, ADDRESS, CREATE_USER, CREATE_DATETIME, UPDATE_DATETIME, UPDATE_USER, DESCRIPTION, UPDATE_NUMBER2, LAST_MODIFIED, PARENT_ID) VALUES (?, 'VIE', 'TEST DIA BAN', TO_DATE('2025-06-05 09:22:51', 'YYYY-MM-DD HH24:MI:SS'), 'M', 'Việt Nam', NULL, '1', 'HNI0400060', 'HNI', NULL, '040', '0060', NULL, NULL, 'Tổ 1-Bồ Đề', NULL, 'Tổ 1-Bồ Đề, Phường Bồ Đề Thành phố Hà Nội', 'VTT1', TO_DATE('2025-06-12 14:02:28', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('2025-10-14 18:17:39', 'YYYY-MM-DD HH24:MI:SS'), 'VTT1', NULL, NULL, NULL, NULL)";
    static final String CALL_INSERT_CUSTOMER_BATCH = "{ call INSERT_CUSTOMER_BATCH(?) }";
    static final String TYPE_CUST_ID_TAB = "T_CUST_ID_TAB";

    private static final Logger LOGGER = Logger.getLogger(DataUtil.class.getName());

    private static final AtomicLong ORA45582_COUNT = new AtomicLong(0);
    private static final AtomicLong LAST_POOL_REFRESH_MS = new AtomicLong(0);
    private static final long POOL_REFRESH_COOLDOWN_MS = 5_000;

    private static final AtomicLong ORA05086_COUNT = new AtomicLong(0);

    private DataUtil() {
    }

    static String env(String k) {
        String v = System.getenv(k);
        if (v == null || v.isEmpty()) throw new IllegalStateException("Missing env: " + k);
        return v;
    }

    static PoolDataSource pool(String urlEnvKey, int threads) throws Exception {
        int poolSize = Math.multiplyExact(threads, Integer.parseInt(env("SHARDS")));

        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        pds.setFastConnectionFailoverEnabled(true);
        pds.setConnectionPoolName("UCP_" + urlEnvKey);
        pds.setURL(env(urlEnvKey));
        pds.setUser(env("GDD_USERNAME"));
        pds.setPassword(env("GDD_PASSWORD"));
        pds.setInitialPoolSize(poolSize);
        pds.setMinPoolSize(poolSize);
        pds.setMaxPoolSize(poolSize);
        return pds;
    }

    static OracleShardingKey shardingKey(PoolDataSource pds, Object id, OracleType type) throws Exception {
        return pds.createShardingKeyBuilder().subkey(id, type).build();
    }

    static Connection openByNumberKey(PoolDataSource gds, BigDecimal key) throws Exception {
        return gds.createConnectionBuilder().shardingKey(shardingKey(gds, key, OracleType.NUMBER)).build();
    }

    static Connection openByRawKey(PoolDataSource gds, byte[] key) throws Exception {
        return gds.createConnectionBuilder().shardingKey(shardingKey(gds, key, OracleType.RAW)).build();
    }

    static boolean isOra45582(SQLException e) {
        if (e.getErrorCode() == 45582) return true;
        String m = e.getMessage();
        return m != null && m.contains("ORA-45582");
    }

    static boolean isOra05086(SQLException e) {
        if (e.getErrorCode() == 5086) return true;
        String m = e.getMessage();
        return m != null && m.contains("ORA-05086");
    }

    static void onOra45582(PoolDataSource pds, String phase, Object key, SQLException cause) {
        long n = ORA45582_COUNT.incrementAndGet();
        LOGGER.log(Level.WARNING, "ORA45582 count={0} phase={1} pool={2} key={3} msg={4}", new Object[]{n, phase, safePoolName(pds), String.valueOf(key), cause.getMessage()});
        refreshPoolWithCooldown(pds, phase);
    }

    static void onOra05086(String phase, Object key, SQLException cause) {
        long n = ORA05086_COUNT.incrementAndGet();
        LOGGER.log(Level.WARNING, "ORA05086 count={0} phase={1} key={2} msg={3}", new Object[]{n, phase, String.valueOf(key), cause.getMessage()});
    }

    private static void refreshPoolWithCooldown(PoolDataSource pds, String phase) {
        long now = System.currentTimeMillis();
        long last = LAST_POOL_REFRESH_MS.get();

        if (now - last < POOL_REFRESH_COOLDOWN_MS) {
            LOGGER.log(Level.WARNING, "ROUTING_REFRESH_SKIPPED phase={0} pool={1} since_ms={2}", new Object[]{phase, safePoolName(pds), (now - last)});
            return;
        }

        if (!LAST_POOL_REFRESH_MS.compareAndSet(last, now)) return;

        try {
            UniversalConnectionPoolManager mgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
            mgr.refreshConnectionPool(pds.getConnectionPoolName());
            LOGGER.log(Level.WARNING, "ROUTING_REFRESH_OK phase={0} pool={1}", new Object[]{phase, safePoolName(pds)});
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "ROUTING_REFRESH_FAIL phase=" + phase + " pool=" + safePoolName(pds), ex);
        }
    }

    private static String safePoolName(PoolDataSource pds) {
        try {
            return pds.getConnectionPoolName();
        } catch (Exception e) {
            return "UNKNOWN_POOL";
        }
    }

    static String resolveShardNameByNumberKey(PoolDataSource gds, BigDecimal key) throws Exception {
        int maxRetry = 3;
        long backoffMs = 100;

        for (int attempt = 1; attempt <= maxRetry; attempt++) {
            try (Connection conn = openByNumberKey(gds, key); PreparedStatement ps = conn.prepareStatement("SELECT name FROM v$database"); ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            } catch (SQLException e) {
                if (isOra45582(e)) {
                    onOra45582(gds, "resolve_shard_name", key, e);
                    if (attempt == maxRetry) throw e;
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 1000);
                    continue;
                }
                throw e;
            }
        }

        throw new IllegalStateException("unreachable");
    }

    static void insertSubscriberByUuid(PoolDataSource gds, byte[] id) throws Exception {
        try (Connection conn = openByRawKey(gds, id); PreparedStatement ps = conn.prepareStatement(INSERT_SQL_UUID)) {
            conn.setAutoCommit(false);
            ps.setBytes(1, id);
            ps.executeUpdate();
            conn.commit();
        }
    }

    static void insertCustomerById(PoolDataSource gds, BigDecimal id) throws Exception {
        try (Connection conn = openByNumberKey(gds, id); PreparedStatement ps = conn.prepareStatement(INSERT_SQL_CUSTOMER)) {
            conn.setAutoCommit(false);
            ps.setBigDecimal(1, id);
            ps.executeUpdate();
            conn.commit();
        }
    }

    static void callInsertCustomerBatch(PoolDataSource gds, BigDecimal routeKey, BigDecimal[] custIds) throws Exception {
        try (Connection conn = openByNumberKey(gds, routeKey); CallableStatement cs = conn.prepareCall(CALL_INSERT_CUSTOMER_BATCH)) {

            conn.setAutoCommit(false);

            OracleConnection oc = conn.unwrap(OracleConnection.class);
            Array arr = oc.createOracleArray(TYPE_CUST_ID_TAB, custIds);

            cs.setArray(1, arr);
            cs.execute();

            conn.commit();
        }
    }

    static void callInsertCustomerBatchWithRetry(PoolDataSource gds, BigDecimal routeKey, BigDecimal[] custIds) throws Exception {
        int maxRetry = 3;
        long backoffMs = 200;

        for (int attempt = 1; attempt <= maxRetry; attempt++) {
            try {
                callInsertCustomerBatch(gds, routeKey, custIds);
                return;
            } catch (SQLException e) {
                if (isOra45582(e)) {
                    onOra45582(gds, "call_insert_customer_batch", routeKey, e);
                    if (attempt == maxRetry) throw e;
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5000);
                    continue;
                }
                if (isOra05086(e)) {
                    onOra05086("call_insert_customer_batch", routeKey, e);
                    if (attempt == maxRetry) throw e;
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5000);
                    continue;
                }
                throw e;
            }
        }

        throw new IllegalStateException("unreachable");
    }

    static void runLoop(int threads, long durationSeconds, Runnable once) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
        ExecutorService workers = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            workers.submit(() -> {
                while (System.nanoTime() < deadline) once.run();
            });
        }

        workers.shutdown();
        workers.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
