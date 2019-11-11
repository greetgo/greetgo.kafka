package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.consumer.annotations.ConsumersFolder;
import kz.greetgo.kafka.consumer.annotations.GroupId;
import kz.greetgo.kafka.consumer.annotations.KafkaNotifier;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.core.logger.Logger;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static kz.greetgo.kafka.util.AnnotationUtil.getAnnotation;

public class ConsumerDefinition {

  private final Object controller;
  private final Method method;
  private final String folderPath;
  private final Invoker invoker;
  private final AutoOffsetReset autoOffsetReset;
  private final String groupId;

  @Override
  public String toString() {
    return "ConsumerDefinition{" +
      controller.getClass() + "#" + method.getName() +
      ", folderPath=" + folderPath +
      ", groupId=" + groupId +
      '}';
  }

  public ConsumerDefinition(Object controller, Method method, Logger logger, String hostId) {
    this.controller = controller;
    this.method = method;

    {
      ConsumersFolder consumersFolder = getAnnotation(controller.getClass(), ConsumersFolder.class);
      folderPath = consumersFolder == null ? null : consumersFolder.value();
    }

    invoker = new InvokerBuilder(controller, method, logger).build();

    {
      autoOffsetReset = getAnnotation(method, KafkaNotifier.class) == null
        ? AutoOffsetReset.EARLIEST : AutoOffsetReset.LATEST;
    }

    {
      final String tmpGroupId;
      GroupId annotation = getAnnotation(method, GroupId.class);
      if (annotation != null) {
        tmpGroupId = annotation.value();
      } else {
        tmpGroupId = method.getName();
      }

      groupId = autoOffsetReset == AutoOffsetReset.EARLIEST ? tmpGroupId : tmpGroupId + hostId;
    }
  }

  /**
   * @return папка, где лежит консюмер. Слэши как разделитель. Не должна начинаться и заканчиваться со слэшем.
   * Может быть null - это значит, что папка корневая
   */
  public String getFolderPath() {
    return folderPath;
  }

  public Class<?> getControllerClass() {
    return getController().getClass();
  }

  public Object getController() {
    return controller;
  }

  public Method getMethod() {
    return method;
  }

  public Invoker getInvoker() {
    return invoker;
  }

  /**
   * @return строка для описания в логах этого консюмера
   */
  public String logDisplay() {

    StringBuilder sb = new StringBuilder();
    if (folderPath != null) {
      sb.append(folderPath).append("/");
    }
    sb.append(controller.getClass().getSimpleName());
    sb.append('.');

    {
      String consumerName = getConsumerName();
      if (Objects.equals(consumerName, method.getName())) {
        sb.append('[').append(consumerName).append(']');
      } else {
        sb.append(method.getName()).append('[').append(consumerName).append(']');
      }
    }

    return sb.toString();

  }

  public AutoOffsetReset getAutoOffsetReset() {
    return autoOffsetReset;
  }

  public String getGroupId() {
    return groupId;
  }

  public boolean isAutoCommit() {
    return invoker.isAutoCommit();
  }

  public String getConsumerName() {
    return invoker.getConsumerName();
  }

  public List<String> topicList() {
    Topic topic = getAnnotation(method, Topic.class);
    if (topic == null) {
      throw new RuntimeException("No @" + Topic.class.getSimpleName() + " over " + method);
    }
    return Arrays.stream(topic.value()).collect(Collectors.toList());
  }

  public String getConfigPath() {

    String configName = getConsumerName();

    if (folderPath == null) {
      return configName;
    } else {
      return folderPath + "/" + configName;
    }

  }
}
