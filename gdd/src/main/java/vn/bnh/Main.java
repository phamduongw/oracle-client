package vn.bnh;

public class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            usage();
            return;
        }

        switch (args[0]) {
            case "--read":
                read();
                break;
            case "--write":
                write();
                break;
            default:
                usage();
        }
    }

    private static void read() {
        System.out.println("MODE=READ");
        DataValidator.run();
    }

    private static void write() {
        System.out.println("MODE=WRITE");
        DataGenerator.run();
    }

    private static void usage() {
        System.out.println("Usage:");
        System.out.println("  java -jar vn.bnh.gdd-1.0.0.jar --read");
        System.out.println("  java -jar vn.bnh.gdd-1.0.0.jar --write");
    }
}
