package kz.greetgo.kafka.consumer;

import java.lang.reflect.Method;

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
}
