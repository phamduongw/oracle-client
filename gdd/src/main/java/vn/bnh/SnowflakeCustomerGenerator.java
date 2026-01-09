package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SnowflakeCustomerGenerator {

    private static final Logger LOGGER = Logger.getLogger(SnowflakeCustomerGenerator.class.getName());
    private static final SnowflakeGenerator ID_GENERATOR = new SnowflakeGenerator();

    private SnowflakeCustomerGenerator() {
    }

    public static void run() {
        try {
            int workerThreads = Integer.parseInt(DataGeneratorUtils.requireEnv("THREADS"));
            long durationSeconds = Long.parseLong(DataGeneratorUtils.requireEnv("DURATION"));

            LOGGER.log(Level.INFO, "START mode=snowflake threads={0} duration_s={1}", new Object[]{workerThreads, durationSeconds});

            PoolDataSource gdsPool = DataGeneratorUtils.createPool("GDS_SEQ_URL", workerThreads);

            DataGeneratorUtils.runForDuration(workerThreads, durationSeconds, () -> {
                try {
                    BigDecimal id = BigDecimal.valueOf(ID_GENERATOR.nextId());
                    DataGeneratorUtils.insertCustomerById(gdsPool, id);
                } catch (Exception e) {
                    DataGeneratorUtils.logStopAndExit(LOGGER, "snowflake", e);
                }
            });

            LOGGER.log(Level.INFO, "END mode=snowflake");
        } catch (Exception e) {
            DataGeneratorUtils.logFatalAndExit(LOGGER, "snowflake", e);
        }
    }
}
