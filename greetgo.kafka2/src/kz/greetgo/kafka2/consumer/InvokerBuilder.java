package kz.greetgo.kafka2.consumer;

import java.lang.reflect.Method;

public class InvokerBuilder {

  public InvokerBuilder(Object controller, Method method, ErrorCatcher errorCatcher) {}

  public Invoker build() {
    throw new RuntimeException("Надо сделать");
  }
}
