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

public final class ShardQueryRunner {

    private static final Logger LOGGER = Logger.getLogger(ShardQueryRunner.class.getName());

    private ShardQueryRunner() {
    }

    public static void run(String[] args) throws Exception {
        String sql = null;
        BigDecimal shardKey = null;

        for (int i = 0; i < args.length; i++) {
            if ("--query".equals(args[i])) {
                sql = args[++i];
                continue;
            }
            if ("--sharding-key".equals(args[i])) {
                shardKey = new BigDecimal(args[++i]);
            }
        }

        if (sql == null) throw new IllegalArgumentException("Missing --query");
        if (shardKey == null) throw new IllegalArgumentException("Missing --sharding-key");

        PoolDataSource gdsPool = DataGeneratorUtils.createPool("GDS_PROC_URL", 1);

        try (Connection shardConn = DataGeneratorUtils.openConnectionByShardKey(gdsPool, shardKey); PreparedStatement shardPs = shardConn.prepareStatement("SELECT name FROM v$database"); ResultSet shardRs = shardPs.executeQuery()) {

            shardRs.next();
            LOGGER.log(Level.INFO, "SHARD={0}", shardRs.getString(1));

            try (PreparedStatement ps = shardConn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                int rowCount = 0;

                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int c = 1; c <= columnCount; c++) {
                        row.put(meta.getColumnLabel(c), rs.getObject(c));
                    }
                    LOGGER.log(Level.INFO, "ROW={0}", row);
                    rowCount++;
                }

                LOGGER.log(Level.INFO, "ROWS={0}", rowCount);
            }
        }
    }
}
