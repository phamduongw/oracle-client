package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataGeneratorSnowflake {

    private static final Logger LOGGER = Logger.getLogger(DataGeneratorSnowflake.class.getName());
    private static final SnowflakeGenerator SNOWFLAKE = new SnowflakeGenerator();

    private DataGeneratorSnowflake() {
    }

    public static void run() {
        try {
            int threads = Integer.parseInt(DataUtil.env("THREADS"));
            long duration = Long.parseLong(DataUtil.env("DURATION"));

            LOGGER.log(Level.INFO, "START mode=snowflake threads={0} duration_s={1}", new Object[]{threads, duration});

            PoolDataSource gds = DataUtil.pool("GDS_SEQ_URL", threads);

            DataUtil.runLoop(threads, duration, () -> {
                try {
                    BigDecimal id = BigDecimal.valueOf(SNOWFLAKE.nextId());
                    DataUtil.insertCustomerById(gds, id);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "STOP mode=snowflake", e);
                    System.exit(1);
                }
            });

            LOGGER.log(Level.INFO, "END mode=snowflake");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL mode=snowflake", e);
            System.exit(1);
        }
    }
}
