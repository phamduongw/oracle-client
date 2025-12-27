package vn.bnh;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 0 || "--uuid".equals(args[0])) {
            DataGeneratorUuid.run();
            return;
        }
        if ("--seq".equals(args[0])) {
            DataGeneratorSeq.run();
            return;
        }
        throw new IllegalArgumentException("Usage: --uuid | --seq");
    }
}
