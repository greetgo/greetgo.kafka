package kz.greetgo.kafka2.consumer;

import java.lang.reflect.Method;

public class InvokerBuilder {
  private final Object controller;
  private final Method method;

  public InvokerBuilder(Object controller, Method method) {
    this.controller = controller;
    this.method = method;
  }

  public Invoker build() {
    throw new RuntimeException("Надо сделать");
  }
}
