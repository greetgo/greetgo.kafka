package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.consumer.annotations.Author;
import kz.greetgo.kafka.consumer.annotations.ConsumerName;
import kz.greetgo.kafka.consumer.annotations.KafkaCommitOn;
import kz.greetgo.kafka.consumer.annotations.Offset;
import kz.greetgo.kafka.consumer.annotations.Partition;
import kz.greetgo.kafka.consumer.annotations.Timestamp;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.errors.IllegalParameterType;
import kz.greetgo.kafka.model.Box;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static kz.greetgo.kafka.util.GenericUtil.extractClass;
import static kz.greetgo.kafka.util.GenericUtil.isOfClass;

public class InvokerBuilder {

  private final Object controller;
  private final Method method;
  private final ConsumerLogger consumerLogger;

  public InvokerBuilder(Object controller, Method method, ConsumerLogger consumerLogger) {
    this.controller = controller;
    this.method = method;
    this.consumerLogger = consumerLogger;
  }

  public Invoker build() {

    Topic topic = method.getAnnotation(Topic.class);
    if (topic == null) {
      throw new IllegalStateException("No annotation Topic for " + method);
    }

    String tmpConsumerName = method.getName();
    {
      ConsumerName annotation = method.getAnnotation(ConsumerName.class);
      if (annotation != null) {
        tmpConsumerName = annotation.value();
      }
    }
    final String consumerName = tmpConsumerName;

    Class<?>[] tmpCommitOn = new Class<?>[0];
    {
      KafkaCommitOn commitOn = method.getAnnotation(KafkaCommitOn.class);
      if (commitOn != null) {
        tmpCommitOn = commitOn.value();
      }
    }
    final Class<?>[] commitOn = tmpCommitOn;

    final Set<String> topicSet = Arrays.stream(topic.value()).collect(Collectors.toSet());

    Type[] parameterTypes = method.getGenericParameterTypes();
    Annotation[][] parameterAnnotations = method.getParameterAnnotations();
    assert parameterTypes.length == parameterAnnotations.length;

    final int parametersCount = parameterTypes.length;

    ParameterValueReader[] parameterValueReaders = new ParameterValueReader[parametersCount];

    for (int i = 0; i < parametersCount; i++) {
      parameterValueReaders[i] = createParameterValueReader(parameterTypes[i], parameterAnnotations[i]);
    }

    Class<?> tmpGettingBodyClass = null;
    for (ParameterValueReader parameterValueReader : parameterValueReaders) {
      Class<?> aClass = parameterValueReader.gettingBodyClass();
      if (aClass != null) {
        tmpGettingBodyClass = aClass;
      }
    }

    final Class<?> gettingBodyClass = tmpGettingBodyClass;

    return new Invoker() {
      @Override
      public boolean invoke(ConsumerRecords<byte[], Box> records) {

        boolean invokedOk = true;

        for (ConsumerRecord<byte[], Box> record : records) {

          if (!isInFilter(record)) {
            continue;
          }

          Object[] parameters = new Object[parametersCount];

          for (int i = 0; i < parametersCount; i++) {
            parameters[i] = parameterValueReaders[i].read(record);
          }

          if (!invokeMethod(parameters)) {
            invokedOk = false;
          }

        }

        return invokedOk;
      }

      private boolean invokeMethod(Object[] parameters) {
        try {
          method.invoke(controller, parameters);
          return true;
        } catch (IllegalAccessException e) {
          consumerLogger.illegalAccessExceptionInvokingMethod(e);
          return false;
        } catch (InvocationTargetException e) {
          Throwable error = e.getTargetException();
          consumerLogger.errorInMethod(error);

          for (Class<?> aClass : commitOn) {
            if (aClass.isInstance(error)) {
              return true;
            }
          }

          return false;
        }
      }

      boolean isInFilter(ConsumerRecord<byte[], Box> record) {
        if (!topicSet.contains(record.topic())) {
          return false;
        }

        if (gettingBodyClass != null) {
          if (gettingBodyClass == Box.class) {
            return true;
          }
          if (!gettingBodyClass.isInstance(record.value().body)) {
            return false;
          }
        }

        Box box = record.value();
        if (box == null) {
          return false;
        }

        {
          List<String> ignorableConsumers = box.ignorableConsumers;
          //noinspection RedundantIfStatement
          if (ignorableConsumers != null && ignorableConsumers.contains(consumerName)) {
            return false;
          }
        }

        return true;
      }

      @Override
      public boolean isAutoCommit() {
        return false;//while always false
      }

      @Override
      public String getConsumerName() {
        return consumerName;
      }
    };
  }

  private ParameterValueReader createParameterValueReader(Type parameterType, Annotation[] parameterAnnotations) {

    for (Annotation annotation : parameterAnnotations) {

      if (annotation instanceof Partition) {
        if (!isOfClass(parameterType, int.class) && !isOfClass(parameterType, Integer.class)) {
          throw new IllegalParameterType("Parameter with @Partition must be `int` or `Integer`");
        }
        return ConsumerRecord::partition;
      }

      if (annotation instanceof Offset) {
        if (!isOfClass(parameterType, long.class) && !isOfClass(parameterType, Long.class)) {
          throw new IllegalParameterType("Parameter with @Offset must be `long` or `Long`");
        }
        return ConsumerRecord::offset;
      }

      if (annotation instanceof Timestamp) {
        if (isOfClass(parameterType, Date.class)) {
          return record -> new Date(record.timestamp());
        }
        if (isOfClass(parameterType, long.class) || isOfClass(parameterType, Long.class)) {
          return ConsumerRecord::timestamp;
        }

        throw new IllegalParameterType("Parameter with @Offset must be `long` or `Long` or `java.util.Date`");
      }

      if (annotation instanceof Author) {
        if (!isOfClass(parameterType, String.class)) {
          throw new IllegalParameterType("Parameter with @Author must be `String`");
        }

        return record -> record.value().author;
      }
    }

    if (isOfClass(parameterType, Box.class)) {
      return ConsumerRecord::value;
    }

    return new ParameterValueReader() {
      @Override
      public Object read(ConsumerRecord<byte[], Box> record) {
        return record.value().body;
      }

      @Override
      public Class<?> gettingBodyClass() {
        return extractClass(parameterType);
      }
    };
  }
}
