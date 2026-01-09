package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SequenceCustomerGenerator {

    private static final Logger LOGGER = Logger.getLogger(SequenceCustomerGenerator.class.getName());
    private static final String SQL_NEXTVAL = "SELECT DATA_SUBCRIBER_SEQ.NEXTVAL FROM DUAL";

    private SequenceCustomerGenerator() {
    }

    private static BigDecimal fetchNextId(PoolDataSource catalogPool) throws Exception {
        try (Connection conn = catalogPool.getConnection(); PreparedStatement ps = conn.prepareStatement(SQL_NEXTVAL); ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getBigDecimal(1);
        }
    }

    public static void run() {
        try {
            int workerThreads = Integer.parseInt(DataGeneratorUtils.requireEnv("THREADS"));
            long durationSeconds = Long.parseLong(DataGeneratorUtils.requireEnv("DURATION"));

            LOGGER.log(Level.INFO, "START mode=sequence threads={0} duration_s={1}", new Object[]{workerThreads, durationSeconds});

            PoolDataSource catalogPool = DataGeneratorUtils.createPool("CATALOG_URL", workerThreads);
            PoolDataSource gdsPool = DataGeneratorUtils.createPool("GDS_SEQ_URL", workerThreads);

            DataGeneratorUtils.runForDuration(workerThreads, durationSeconds, () -> {
                try {
                    BigDecimal id = fetchNextId(catalogPool);
                    DataGeneratorUtils.insertCustomerById(gdsPool, id);
                } catch (Exception e) {
                    DataGeneratorUtils.logStopAndExit(LOGGER, "sequence", e);
                }
            });

            LOGGER.log(Level.INFO, "END mode=sequence");
        } catch (Exception e) {
            DataGeneratorUtils.logFatalAndExit(LOGGER, "sequence", e);
        }
    }
}
