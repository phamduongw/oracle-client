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

public final class ProcedureBatchCustomerGenerator {

    private static final Logger LOGGER = Logger.getLogger(ProcedureBatchCustomerGenerator.class.getName());
    private static final SnowflakeGenerator ID_GENERATOR = new SnowflakeGenerator();

    private ProcedureBatchCustomerGenerator() {
    }

    public static void run() {
        ExecutorService shardExecutor = null;

        try {
            int workerThreads = Integer.parseInt(DataGeneratorUtils.requireEnv("THREADS"));
            long durationSeconds = Long.parseLong(DataGeneratorUtils.requireEnv("DURATION"));
            int batchSize = Integer.parseInt(DataGeneratorUtils.requireEnv("BATCH_SIZE"));
            int shardParallelism = Integer.parseInt(DataGeneratorUtils.requireEnv("SHARDS"));

            LOGGER.log(Level.INFO, "START mode=procedure_batch threads={0} duration_s={1} batch_size={2}", new Object[]{workerThreads, durationSeconds, batchSize});

            PoolDataSource gdsPool = DataGeneratorUtils.createPool("GDS_PROC_URL", workerThreads);
            shardExecutor = Executors.newFixedThreadPool(Math.max(1, shardParallelism));

            ExecutorService finalShardExecutor = shardExecutor;

            DataGeneratorUtils.runForDuration(workerThreads, durationSeconds, () -> {
                try {
                    List<BigDecimal> batchIds = new ArrayList<>(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        batchIds.add(BigDecimal.valueOf(ID_GENERATOR.nextId()));
                    }

                    Map<String, List<BigDecimal>> shardToIds = new LinkedHashMap<>();
                    for (BigDecimal id : batchIds) {
                        String shardName = DataGeneratorUtils.resolveShardNameByShardKey(gdsPool, id);
                        shardToIds.computeIfAbsent(shardName, k -> new ArrayList<>()).add(id);
                    }

                    if (shardToIds.isEmpty()) return;

                    List<Future<?>> futures = new ArrayList<>(shardToIds.size());

                    for (Map.Entry<String, List<BigDecimal>> entry : shardToIds.entrySet()) {
                        String shardName = entry.getKey();
                        List<BigDecimal> ids = entry.getValue();

                        BigDecimal routeKey = ids.get(0);
                        BigDecimal[] customerIds = ids.toArray(new BigDecimal[0]);

                        futures.add(finalShardExecutor.submit(() -> {
                            try {
                                DataGeneratorUtils.callInsertCustomerBatchWithRetry(gdsPool, routeKey, customerIds);
                                LOGGER.log(Level.INFO, "BATCH_OK shard={0} size={1}", new Object[]{shardName, customerIds.length});
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }));
                    }

                    for (Future<?> f : futures) {
                        try {
                            f.get();
                        } catch (ExecutionException ex) {
                            Throwable root = ex.getCause() == null ? ex : ex.getCause();
                            DataGeneratorUtils.logStopAndExit(LOGGER, "procedure_batch", root);
                        }
                    }
                } catch (Exception e) {
                    DataGeneratorUtils.logStopAndExit(LOGGER, "procedure_batch", e);
                }
            });

            LOGGER.log(Level.INFO, "END mode=procedure_batch");
        } catch (Exception e) {
            DataGeneratorUtils.logFatalAndExit(LOGGER, "procedure_batch", e);
        } finally {
            if (shardExecutor != null) shardExecutor.shutdownNow();
        }
    }
}
