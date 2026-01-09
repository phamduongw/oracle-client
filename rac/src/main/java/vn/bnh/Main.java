package vn.bnh;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 0) throw new IllegalArgumentException("Usage: --sequence");

        if ("--sequence".equals(args[0])) {
            DataGeneratorSequence.run();
            return;
        }

        throw new IllegalArgumentException("Usage: --sequence");
    }
}
