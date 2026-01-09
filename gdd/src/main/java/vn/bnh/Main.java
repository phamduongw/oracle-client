package vn.bnh;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Usage: --sequence | --snowflake | --uuid | --procedure | --query \"...\" --sharding-key \"...\"");
        }

        switch (args[0]) {
            case "--sequence":
                DataGeneratorSequence.run();
                return;
            case "--snowflake":
                DataGeneratorSnowflake.run();
                return;
            case "--uuid":
                DataGeneratorUuid.run();
                return;
            case "--procedure":
                DataGeneratorProcedure.run();
                return;
            default:
                break;
        }

        try {
            DataChecker.run(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
