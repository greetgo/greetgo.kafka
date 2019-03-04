package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.consumer.annotations.ConsumersFolder;
import kz.greetgo.kafka2.model.Box;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.lang.reflect.Method;

public class ConsumerDefinition {

  private final Object controller;
  private final String folderPath;
  private final Invoker invoker;

  public ConsumerDefinition(Object controller, Method method, ConsumerLogger consumerLogger, String hostId) {
    this.controller = controller;

    {
      ConsumersFolder consumersFolder = controller.getClass().getAnnotation(ConsumersFolder.class);
      folderPath = consumersFolder == null ? null : consumersFolder.value();
    }

    invoker = new InvokerBuilder(controller, method, consumerLogger).build();
  }

  /**
   * @return папка, где лежит консюмер. Слэши как разделитель. Не должна начинаться и заканчиваться со слэшем.
   * Может быть null - это значит, что папка корневая
   */
  public String getFolderPath() {
    return folderPath;
  }

  public Class<?> getControllerClass() {
    return controller.getClass();
  }

  public boolean invoke(ConsumerRecords<byte[], Box> records) {
    return invoker.invoke(records);
  }

  /**
   * @return строка для описания в логах этого консюмера
   */
  public String logDisplay() {
    throw new RuntimeException("Надо сделать");
  }

  public AutoOffsetReset getAutoOffsetReset() {
    throw new RuntimeException("Надо сделать");
  }

  public String getGroupId() {
    throw new RuntimeException("Надо сделать");
  }

  public boolean isAutoCommit() {
    return invoker.isAutoCommit();
  }

  public String getConsumerName() {
    return invoker.getConsumerName();
  }
}
