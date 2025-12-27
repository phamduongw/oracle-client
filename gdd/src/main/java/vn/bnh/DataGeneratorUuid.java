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
            int threads = Integer.parseInt(GenUtil.env("THREADS"));
            long duration = Long.parseLong(GenUtil.env("DURATION"));
            PoolDataSource gds = GenUtil.pool("GDS_UUID_URL", threads);

            GenUtil.runLoop(threads, duration, () -> {
                try {
                    GenUtil.insertUuid(gds, raw16());
                } catch (Exception e) {
                    GenUtil.ERR.incrementAndGet();
                    LOGGER.log(Level.SEVERE, "STOP mode=uuid step=insert", e);
                    System.exit(1);
                }
            }, "uuid");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL mode=uuid", e);
            System.exit(1);
        }
    }
}
