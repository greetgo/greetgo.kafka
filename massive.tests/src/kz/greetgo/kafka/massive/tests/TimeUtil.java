package kz.greetgo.kafka.massive.tests;

public class TimeUtil {

  public static final double GIG = 1e9;

  public static String nanosRead(long nanos) {
    return "" + (nanos / GIG) + "s";
  }

  public static void main(String[] args) throws InterruptedException {
    long started = System.nanoTime();
    Thread.sleep(1000);
    System.out.println("delay = " + nanosRead(System.nanoTime() - started));
  }
}
