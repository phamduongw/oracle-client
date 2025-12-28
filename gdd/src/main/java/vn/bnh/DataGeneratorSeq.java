package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataGeneratorSeq {

    private static final Logger LOGGER = Logger.getLogger(DataGeneratorSeq.class.getName());
    private static final String NEXTVAL_SQL = "SELECT DATA_SUBCRIBER_ID_SEQ.NEXTVAL FROM DUAL";

    private DataGeneratorSeq() {
    }

    private static BigDecimal nextId(PoolDataSource catalog) throws Exception {
        try (Connection conn = catalog.getConnection(); PreparedStatement ps = conn.prepareStatement(NEXTVAL_SQL); ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getBigDecimal(1);
        }
    }

    public static void run() {
        try {
            int threads = Integer.parseInt(GenUtil.env("THREADS"));
            long duration = Long.parseLong(GenUtil.env("DURATION"));
            PoolDataSource catalog = GenUtil.pool("CATALOG_URL", threads);
            PoolDataSource gds = GenUtil.pool("GDS_SEQ_URL", threads);

            GenUtil.runLoop(threads, duration, () -> {
                BigDecimal id;
                try {
                    id = nextId(catalog);
                } catch (Exception e) {
                    GenUtil.ERR.incrementAndGet();
                    LOGGER.log(Level.SEVERE, "STOP mode=seq step=nextval", e);
                    System.exit(1);
                    return;
                }
                try {
                    GenUtil.insertSeq(gds, id);
                } catch (Exception e) {
                    GenUtil.ERR.incrementAndGet();
                    LOGGER.log(Level.SEVERE, "STOP mode=seq step=insert id=" + id, e);
                    System.exit(1);
                }
            }, "seq");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL mode=seq", e);
            System.exit(1);
        }
    }
}
