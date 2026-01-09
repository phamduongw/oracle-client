package vn.bnh;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochRandomGenerator;
import oracle.ucp.jdbc.PoolDataSource;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataGeneratorUuid {

    private static final Logger LOGGER = Logger.getLogger(DataGeneratorUuid.class.getName());
    private static final TimeBasedEpochRandomGenerator UUID_GEN = Generators.timeBasedEpochRandomGenerator();

    private DataGeneratorUuid() {
    }

    private static byte[] raw16() {
        UUID u = UUID_GEN.generate();
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(u.getMostSignificantBits());
        b.putLong(u.getLeastSignificantBits());
        return b.array();
    }

    public static void run() {
        try {
            int threads = Integer.parseInt(DataUtil.env("THREADS"));
            long duration = Long.parseLong(DataUtil.env("DURATION"));

            LOGGER.log(Level.INFO, "START mode=uuid threads={0} duration_s={1}", new Object[]{threads, duration});

            PoolDataSource gds = DataUtil.pool("GDS_UUID_URL", threads);

            DataUtil.runLoop(threads, duration, () -> {
                try {
                    DataUtil.insertSubscriberByUuid(gds, raw16());
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "STOP mode=uuid", e);
                    System.exit(1);
                }
            });

            LOGGER.log(Level.INFO, "END mode=uuid");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL mode=uuid", e);
            System.exit(1);
        }
    }
}
