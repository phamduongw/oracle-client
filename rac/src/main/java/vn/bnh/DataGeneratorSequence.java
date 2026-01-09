package vn.bnh;

import oracle.ucp.jdbc.PoolDataSource;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataGeneratorSequence {

    private static final Logger LOGGER = Logger.getLogger(DataGeneratorSequence.class.getName());

    private DataGeneratorSequence() {
    }

    public static void run() {
        try {
            int threads = Integer.parseInt(GenUtil.env("THREADS"));
            long duration = Long.parseLong(GenUtil.env("DURATION"));

            LOGGER.log(Level.INFO, "START mode=sequence threads={0} duration_s={1}", new Object[]{threads, duration});

            PoolDataSource db = GenUtil.pool(threads);

            GenUtil.runLoop(threads, duration, () -> {
                try {
                    GenUtil.insertSeq(db);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "STOP mode=sequence", e);
                    System.exit(1);
                }
            });

            LOGGER.log(Level.INFO, "END mode=sequence");
        } catch (Exception e) {
            Logger.getLogger(DataGeneratorSequence.class.getName()).log(Level.SEVERE, "FATAL mode=sequence", e);
            System.exit(1);
        }
    }
}
