package kz.greetgo.kafka.model;

import java.util.List;

public interface BoxHolder<T> {

  String author();

  List<String> ignorableConsumers();

  T body();

}
