package kz.greetgo.kafka_old.consumer;

import kz.greetgo.kafka_old.core.BoxRecord;

import java.util.List;

public interface Caller {
  void call(List<BoxRecord> list);
}
