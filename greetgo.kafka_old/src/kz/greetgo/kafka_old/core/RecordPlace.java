package kz.greetgo.kafka_old.core;

public class RecordPlace {
  public final String topic;

  public final int partition;

  public final long offset;

  public RecordPlace(String topic, int partition, long offset) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
  }
}
