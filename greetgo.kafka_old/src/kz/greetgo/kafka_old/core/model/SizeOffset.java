package kz.greetgo.kafka_old.core.model;

public class SizeOffset {
  public long size, offset;

  @Override
  public String toString() {
    return "SizeOffset{" +
        "size=" + size +
        ", offset=" + offset +
        '}';
  }
}
