package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.model.Box;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.lang.reflect.Method;

public class ConsumerDefinition {

  private final Object controller;
  private final Method method;

  public ConsumerDefinition(Object controller, Method method) {
    this.controller = controller;
    this.method = method;
  }

  /**
   * @return папка, где лежит консюмер. Слэши как разделитель. Не должна начинаться и заканчиваться со слэшем.
   * Может быть null - это значит, что папка корневая
   */
  public String getFolderPath() {
    return null;
  }

  public Class<?> getControllerClass() {
    return controller.getClass();
  }

  public void invoke(ConsumerRecords<byte[], Box> records) {

  }

  /**
   * @return строка для описания в логах этого консюмера
   */
  public String logDisplay() {
    throw new RuntimeException("Надо сделать");
  }
}
