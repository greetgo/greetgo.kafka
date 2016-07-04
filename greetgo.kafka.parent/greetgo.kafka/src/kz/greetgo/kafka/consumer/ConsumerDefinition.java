package kz.greetgo.kafka.consumer;

import java.lang.reflect.Method;
import java.util.Arrays;

public class ConsumerDefinition {
  public final Object bean;
  public final Method method;
  public final Consume consume;
  public final Caller caller;

  public ConsumerDefinition(Object bean, Method method, Consume consume) {
    this.bean = bean;
    this.method = method;
    this.consume = consume;
    this.caller = UtilCaller.createCaller(bean, method);
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
