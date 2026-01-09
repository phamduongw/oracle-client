package vn.bnh;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Usage: --sequence | --snowflake | --procedure | --query \"...\" --sharding-key \"...\"");
        }

        switch (args[0]) {
            case "--sequence":
                SequenceCustomerGenerator.run();
                return;
            case "--snowflake":
                SnowflakeCustomerGenerator.run();
                return;
            case "--procedure":
                ProcedureBatchCustomerGenerator.run();
                return;
            default:
                break;
        }

        try {
            ShardQueryRunner.run(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
