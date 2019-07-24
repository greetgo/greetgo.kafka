package kz.greetgo.kafka.massive.tests;

public class Probe {
  public static void main(String[] args) throws InterruptedException {

    {
      long second = System.nanoTime()/ 1000_000_000L;
      System.out.println("second = " + second);
    }

    Thread.sleep(1000);
    {
      long nanoTime = System.nanoTime();
      long second = nanoTime / 1000_000_000L;
      System.out.println("second = " + second);
    }

    Thread.sleep(1000);
    {
      long nanoTime = System.nanoTime();
      long second = nanoTime / 1000_000_000L;
      System.out.println("second = " + second);
    }

    Thread.sleep(1000);
    {
      long nanoTime = System.nanoTime();
      long second = nanoTime / 1000_000_000L;
      System.out.println("second = " + second);
    }

  }
}
