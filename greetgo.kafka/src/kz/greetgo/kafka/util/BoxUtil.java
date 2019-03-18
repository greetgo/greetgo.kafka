package kz.greetgo.kafka.util;

import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.model.BoxHolder;

import java.util.List;
import java.util.Optional;

public class BoxUtil {

  public static <T> Optional<BoxHolder<T>> hold(Box box, Class<T> aClass) {
    if (box == null) {
      return Optional.empty();
    }

    if (aClass.isInstance(box.body)) {
      return Optional.of(new BoxHolder<T>() {
        @Override
        public String author() {
          return box.author;
        }

        @Override
        public List<String> ignorableConsumers() {
          return box.ignorableConsumers;
        }

        @Override
        public T body() {
          //noinspection unchecked
          return (T) box.body;
        }
      });
    }

    return Optional.empty();
  }

}
