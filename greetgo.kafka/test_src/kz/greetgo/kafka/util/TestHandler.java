package kz.greetgo.kafka.util;

public class TestHandler implements Handler {
  public int happenCount = 0;

  @Override
  public void handler() {
    happenCount++;
  }
}
