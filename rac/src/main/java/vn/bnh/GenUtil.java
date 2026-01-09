package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

final class GenUtil {

    static final String INSERT_SQL_SEQ = "INSERT INTO CUSTOMER_T1 (CUST_TYPE, NAME, BIRTH_DATE, SEX, NATIONALITY, VIP, STATUS, AREA_CODE, PROVINCE, DISTRICT, PRECINCT, STREET_BLOCK, STREET, STREET_NAME, STREET_BLOCK_NAME, HOME, ADDRESS, CREATE_USER, CREATE_DATETIME, UPDATE_DATETIME, UPDATE_USER, DESCRIPTION, UPDATE_NUMBER2, LAST_MODIFIED, PARENT_ID) VALUES ('VIE', 'TEST DIA BAN', TO_DATE('2025-06-05 09:22:51', 'YYYY-MM-DD HH24:MI:SS'), 'M', 'Việt Nam', NULL, '1', 'HNI0400060', 'HNI', NULL, '040', '0060', NULL, NULL, 'Tổ 1-Bồ Đề', NULL, 'Tổ 1-Bồ Đề, Phường Bồ Đề Thành phố Hà Nội', 'VTT1', TO_DATE('2025-06-12 14:02:28', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('2025-10-14 18:17:39', 'YYYY-MM-DD HH24:MI:SS'), 'VTT1', NULL, NULL, NULL, NULL)";

    private GenUtil() {
    }

    static String env(String k) {
        String v = System.getenv(k);
        if (v == null || v.isEmpty()) throw new IllegalStateException("Missing env: " + k);
        return v;
    }

    static PoolDataSource pool(int threads) throws Exception {
        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        pds.setURL(env("RAC_URL"));
        pds.setUser(env("RAC_USERNAME"));
        pds.setPassword(env("RAC_PASSWORD"));
        pds.setInitialPoolSize(threads);
        pds.setMinPoolSize(threads);
        pds.setMaxPoolSize(threads);
        return pds;
    }

    static void insertSeq(PoolDataSource db) throws Exception {
        try (Connection conn = db.getConnection(); PreparedStatement ps = conn.prepareStatement(INSERT_SQL_SEQ)) {
            conn.setAutoCommit(false);
            ps.executeUpdate();
            conn.commit();
        }
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
