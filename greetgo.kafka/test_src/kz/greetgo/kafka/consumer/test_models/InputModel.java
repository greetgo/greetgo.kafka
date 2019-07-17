package kz.greetgo.kafka.consumer.test_models;

import kz.greetgo.kafka.core.HasStrKafkaKey;
import kz.greetgo.util.RND;

import java.util.Objects;

public class InputModel implements HasStrKafkaKey {

  public String id;
  public String value;

  @Override
  public String extractStrKafkaKey() {
    return id;
  }

  public static InputModel rnd() {
    InputModel ret = new InputModel();
    ret.id = RND.str(10);
    ret.value = RND.str(10);
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputModel that = (InputModel) o;
    return Objects.equals(id, that.id) &&
      Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, value);
  }
}
