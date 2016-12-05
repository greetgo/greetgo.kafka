package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.BoxRecord;

import java.util.List;

public interface Caller {
  void call(List<BoxRecord> list);
}
