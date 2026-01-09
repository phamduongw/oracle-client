package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataChecker {

    private static final Logger LOGGER = Logger.getLogger(DataChecker.class.getName());

    private DataChecker() {
    }

    public static void run(String[] args) throws Exception {
        String query = null;
        BigDecimal shardingKey = null;

        for (int i = 0; i < args.length; i++) {
            if ("--query".equals(args[i])) {
                query = args[++i];
                continue;
            }
            if ("--sharding-key".equals(args[i])) {
                shardingKey = new BigDecimal(args[++i]);
            }
        }

        if (query == null) throw new IllegalArgumentException("Missing --query");
        if (shardingKey == null) throw new IllegalArgumentException("Missing --sharding-key");

        PoolDataSource gds = DataUtil.pool("GDS_PROC_URL", 1);

        try (Connection conn = DataUtil.openByNumberKey(gds, shardingKey); PreparedStatement psShard = conn.prepareStatement("SELECT name FROM v$database"); ResultSet rsShard = psShard.executeQuery()) {

            rsShard.next();
            LOGGER.log(Level.INFO, "SHARD={0}", rsShard.getString(1));

            try (PreparedStatement ps = conn.prepareStatement(query); ResultSet rs = ps.executeQuery()) {

                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                int rows = 0;

                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int c = 1; c <= cols; c++) row.put(md.getColumnLabel(c), rs.getObject(c));
                    LOGGER.log(Level.INFO, "ROW={0}", row);
                    rows++;
                }

                LOGGER.log(Level.INFO, "ROWS={0}", rows);
            }
        }
    }
}
