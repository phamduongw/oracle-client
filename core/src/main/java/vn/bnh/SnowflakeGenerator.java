package vn.bnh;

public final class SnowflakeGenerator {

    private static final int NODE_ID_BITS = 10;
    private static final int SEQUENCE_BITS = 12;

    private static final int MAX_NODE_ID = (1 << NODE_ID_BITS) - 1;
    private static final int MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1;

    private static final int NODE_SHIFT = SEQUENCE_BITS;
    private static final int TIME_SHIFT = NODE_ID_BITS + SEQUENCE_BITS;

    private static final long CUSTOM_EPOCH = 1420070400000L;

    private final int nodeId;
    private long lastTimestamp = -1L;
    private int sequence = 0;

    public SnowflakeGenerator() {
        int id = Integer.parseInt(System.getenv("MACHINE_ID"));
        if ((id & ~MAX_NODE_ID) != 0) throw new IllegalStateException();
        this.nodeId = id;
    }

    public SnowflakeGenerator(int nodeId) {
        if ((nodeId & ~MAX_NODE_ID) != 0) throw new IllegalArgumentException();
        this.nodeId = nodeId;
    }

    public synchronized long nextId() {
        long ts = System.currentTimeMillis() - CUSTOM_EPOCH;

        while (ts < lastTimestamp) {
            Thread.onSpinWait();
            ts = System.currentTimeMillis() - CUSTOM_EPOCH;
        }

        if (ts == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                do {
                    Thread.onSpinWait();
                    ts = System.currentTimeMillis() - CUSTOM_EPOCH;
                } while (ts == lastTimestamp);
            }
        } else {
            sequence = 0;
        }

        lastTimestamp = ts;
        return (ts << TIME_SHIFT) | ((long) nodeId << NODE_SHIFT) | (sequence & MAX_SEQUENCE);
    }
}
