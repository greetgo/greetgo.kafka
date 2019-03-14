package kz.greetgo.kafka_old.consumer;

import java.lang.reflect.Method;
import java.util.Arrays;

public class ConsumerDefinition {
  public final Object bean;
  public final Method method;
  public final Consume consume;
  public final Caller caller;
  public final boolean addSoulIdToEndOfCursorId;

  public ConsumerDefinition(Object bean, Method method, Consume consume, boolean addSoulIdToEndOfCursorId) {
    this.bean = bean;
    this.method = method;
    this.consume = consume;
    this.addSoulIdToEndOfCursorId = addSoulIdToEndOfCursorId;
    this.caller = CallerCreator.create(bean, method);
  }

  @Override
  public String toString() {
    return "ConsumerDefinition{" +
      ", consume(name=" + consume.name() +
      ", cursorId=" + consume.cursorId() +
      ", topics=" + Arrays.toString(consume.topics()) +
      "), bean=" + bean + ", method=" + method +
      '}';
  }
}
