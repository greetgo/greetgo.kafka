package kz.greetgo.kafka2.consumer;

import java.lang.reflect.Method;

public class InvokerBuilder {

  public InvokerBuilder(Object controller, Method method, ConsumerLogger consumerLogger) {}

  public Invoker build() {
    throw new RuntimeException("Надо сделать");
  }
}
