package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.Box;

import java.util.List;

public interface Caller {
  void call(List<Box> list);
}
