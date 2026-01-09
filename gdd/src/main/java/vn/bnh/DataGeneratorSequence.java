package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataGeneratorSequence {

    private static final Logger LOGGER = Logger.getLogger(DataGeneratorSequence.class.getName());
    private static final String NEXTVAL_SQL = "SELECT DATA_SUBCRIBER_SEQ.NEXTVAL FROM DUAL";

    private DataGeneratorSequence() {
    }

    private static BigDecimal nextId(PoolDataSource catalog) throws Exception {
        try (Connection conn = catalog.getConnection(); PreparedStatement ps = conn.prepareStatement(NEXTVAL_SQL); ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getBigDecimal(1);
        }
    }

    public static void run() {
        try {
            int threads = Integer.parseInt(DataUtil.env("THREADS"));
            long duration = Long.parseLong(DataUtil.env("DURATION"));

            LOGGER.log(Level.INFO, "START mode=sequence threads={0} duration_s={1}", new Object[]{threads, duration});

            PoolDataSource catalog = DataUtil.pool("CATALOG_URL", threads);
            PoolDataSource gds = DataUtil.pool("GDS_SEQ_URL", threads);

            DataUtil.runLoop(threads, duration, () -> {
                try {
                    BigDecimal id = nextId(catalog);
                    DataUtil.insertCustomerById(gds, id);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "STOP mode=sequence", e);
                    System.exit(1);
                }
            });

            LOGGER.log(Level.INFO, "END mode=sequence");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL mode=sequence", e);
            System.exit(1);
        }
    }
}
