package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataGeneratorProcedure {

    private static final Logger LOGGER = Logger.getLogger(DataGeneratorProcedure.class.getName());
    private static final SnowflakeGenerator SNOWFLAKE = new SnowflakeGenerator();

    private DataGeneratorProcedure() {
    }

    public static void run() {
        try {
            int threads = Integer.parseInt(DataUtil.env("THREADS"));
            long duration = Long.parseLong(DataUtil.env("DURATION"));
            int batchSize = Integer.parseInt(DataUtil.env("BATCH_SIZE"));

            LOGGER.log(Level.INFO, "START mode=procedure_batch threads={0} duration_s={1} batch_size={2}", new Object[]{threads, duration, batchSize});

            PoolDataSource gds = DataUtil.pool("GDS_PROC_URL", threads);

            DataUtil.runLoop(threads, duration, () -> {
                try {
                    List<BigDecimal> batch = new ArrayList<>(batchSize);
                    for (int i = 0; i < batchSize; i++) batch.add(BigDecimal.valueOf(SNOWFLAKE.nextId()));

                    Map<String, List<BigDecimal>> byShard = new LinkedHashMap<>();
                    for (BigDecimal id : batch) {
                        String shard = DataUtil.resolveShardNameByNumberKey(gds, id);
                        byShard.computeIfAbsent(shard, k -> new ArrayList<>()).add(id);
                    }

                    if (byShard.isEmpty()) return;

                    ExecutorService exec = Executors.newFixedThreadPool(byShard.size());
                    List<Future<?>> futures = new ArrayList<>(byShard.size());

                    for (Map.Entry<String, List<BigDecimal>> e : byShard.entrySet()) {
                        String shard = e.getKey();
                        List<BigDecimal> ids = e.getValue();
                        BigDecimal routeKey = ids.get(0);
                        BigDecimal[] arr = ids.toArray(new BigDecimal[0]);

                        futures.add(exec.submit(() -> {
                            try {
                                DataUtil.callInsertCustomerBatchWithRetry(gds, routeKey, arr);
                                LOGGER.log(Level.INFO, "BATCH_OK shard={0} size={1}", new Object[]{shard, arr.length});
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }));
                    }

                    exec.shutdown();

                    for (Future<?> f : futures) {
                        try {
                            f.get();
                        } catch (ExecutionException ex) {
                            Throwable root = ex.getCause() == null ? ex : ex.getCause();
                            LOGGER.log(Level.SEVERE, "STOP mode=procedure_batch", root);
                            System.exit(1);
                        }
                    }
                } catch (Exception ex) {
                    LOGGER.log(Level.SEVERE, "STOP mode=procedure_batch", ex);
                    System.exit(1);
                }
            });

            LOGGER.log(Level.INFO, "END mode=procedure_batch");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL mode=procedure_batch", e);
            System.exit(1);
        }
    }
}
