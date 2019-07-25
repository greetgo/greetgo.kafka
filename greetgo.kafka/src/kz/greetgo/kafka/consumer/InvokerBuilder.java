package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.consumer.annotations.Author;
import kz.greetgo.kafka.consumer.annotations.ConsumerName;
import kz.greetgo.kafka.consumer.annotations.InnerProducerName;
import kz.greetgo.kafka.consumer.annotations.KafkaCommitOn;
import kz.greetgo.kafka.consumer.annotations.Offset;
import kz.greetgo.kafka.consumer.annotations.Partition;
import kz.greetgo.kafka.consumer.annotations.Timestamp;
import kz.greetgo.kafka.consumer.annotations.ToTopic;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.consumer.parameters.InnerProducerSenderValueReader;
import kz.greetgo.kafka.consumer.parameters.InnerProducerValueReader;
import kz.greetgo.kafka.core.KafkaReactor;
import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.errors.AbsentAnnotationToTopicOverInnerProducer;
import kz.greetgo.kafka.errors.IllegalParameterType;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.KafkaFuture;
import kz.greetgo.kafka.producer.ProducerFacade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static kz.greetgo.kafka.core.logger.LoggerType.LOG_CONSUMER_ERROR_IN_METHOD;
import static kz.greetgo.kafka.core.logger.LoggerType.LOG_CONSUMER_ILLEGAL_ACCESS_EXCEPTION_INVOKING_METHOD;
import static kz.greetgo.kafka.util.GenericUtil.extractClass;
import static kz.greetgo.kafka.util.GenericUtil.isOfClass;

public class InvokerBuilder {

  private final Object controller;
  private final Method method;
  private final Logger logger;

  public InvokerBuilder(Object controller, Method method, Logger logger) {
    this.controller = controller;
    this.method = method;
    this.logger = logger;
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

    InnerProducerName parentProducerName = method.getAnnotation(InnerProducerName.class);
    if (parentProducerName == null) {
      parentProducerName = controller.getClass().getAnnotation(InnerProducerName.class);
    }

    final Set<String> topicSet = Arrays.stream(topic.value()).collect(Collectors.toSet());

    Type[] parameterTypes = method.getGenericParameterTypes();
    Annotation[][] parameterAnnotations = method.getParameterAnnotations();
    assert parameterTypes.length == parameterAnnotations.length;

    final int parametersCount = parameterTypes.length;

    ParameterValueReader[] parameterValueReaders = new ParameterValueReader[parametersCount];

    for (int i = 0; i < parametersCount; i++) {
      parameterValueReaders[i] = createParameterValueReader(parameterTypes[i], parameterAnnotations[i], parentProducerName);
    }

    Class<?> tmpGettingBodyClass = null;
    for (ParameterValueReader parameterValueReader : parameterValueReaders) {
      Class<?> aClass = parameterValueReader.gettingBodyClass();
      if (aClass != null) {
        tmpGettingBodyClass = aClass;
      }
    }

    final Class<?> gettingBodyClass = tmpGettingBodyClass;

    final Set<String> usingProducerNames = new HashSet<>();

    for (ParameterValueReader parameterValueReader : parameterValueReaders) {
      usingProducerNames.addAll(parameterValueReader.getProducerNames());
    }

    return new Invoker() {

      @Override
      public Set<String> getUsingProducerNames() {
        return usingProducerNames;
      }

      @Override
      public InvokeSession createSession() {
        return new InvokeSession() {

          private final InvokeSessionContext context = new InvokeSessionContext();

          @Override
          public void putProducer(String producerName, ProducerFacade producer) {
            context.putProducer(producerName, producer);
          }

          @Override
          public boolean invoke(ConsumerRecords<byte[], Box> records) {
            boolean invokedOk = true;

            List<KafkaFuture> kafkaFutures = new ArrayList<>();

            for (ConsumerRecord<byte[], Box> record : records) {

              if (!isInFilter(record)) {
                continue;
              }

              context.kafkaFutures.clear();

              Object[] parameters = new Object[parametersCount];

              for (int i = 0; i < parametersCount; i++) {
                parameters[i] = parameterValueReaders[i].read(record, context);
              }

              if (!invokeMethod(parameters)) {
                invokedOk = false;
              }

              for (int i = 0; i < parametersCount; i++) {
                kafkaFutures.addAll(context.kafkaFutures);
              }

              context.kafkaFutures.clear();

            }

            kafkaFutures.stream().filter(Objects::nonNull).forEach(KafkaFuture::awaitAndGet);

            return invokedOk;
          }

          private boolean invokeMethod(Object[] parameters) {
            try {
              method.invoke(controller, parameters);
              return true;
            } catch (IllegalAccessException e) {
              if (logger.isShow(LOG_CONSUMER_ILLEGAL_ACCESS_EXCEPTION_INVOKING_METHOD)) {
                logger.logConsumerIllegalAccessExceptionInvokingMethod(e, consumerName, controller, method);
              }
              return false;
            } catch (InvocationTargetException e) {
              Throwable error = e.getTargetException();
              if (logger.isShow(LOG_CONSUMER_ERROR_IN_METHOD)) {
                logger.logConsumerErrorInMethod(error, consumerName, controller, method);
              }

              for (Class<?> aClass : commitOn) {
                if (aClass.isInstance(error)) {
                  return true;
                }
              }

              return false;
            }
          }

          @Override
          public void close() {
            context.close();
          }
        };
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

  private ParameterValueReader createParameterValueReader(Type parameterType,
                                                          Annotation[] parameterAnnotations,
                                                          InnerProducerName parentProducerName) {

    InnerProducerName producerName = parentProducerName;

    ToTopic toTopic = null;
    AtomicReference<String> finalProducerName = new AtomicReference<>(KafkaReactor.DEFAULT_INNER_PRODUCER_NAME);

    for (Annotation annotation : parameterAnnotations) {

      if (annotation instanceof Partition) {
        if (!isOfClass(parameterType, int.class) && !isOfClass(parameterType, Integer.class)) {
          throw new IllegalParameterType("Parameter with @Partition must be `int` or `Integer`");
        }

        return (record, invokeSessionContext) -> record.partition();
      }

      if (annotation instanceof Offset) {
        if (!isOfClass(parameterType, long.class) && !isOfClass(parameterType, Long.class)) {
          throw new IllegalParameterType("Parameter with @Offset must be `long` or `Long`");
        }

        return (record, invokeSessionContext) -> record.offset();
      }

      if (annotation instanceof Timestamp) {
        if (isOfClass(parameterType, Date.class)) {
          return (record, invokeSessionContext) -> new Date(record.timestamp());
        }
        if (isOfClass(parameterType, long.class) || isOfClass(parameterType, Long.class)) {
          return (record, invokeSessionContext) -> record.timestamp();
        }

        throw new IllegalParameterType("Parameter with @Offset must be `long` or `Long` or `java.util.Date`");
      }

      if (annotation instanceof Author) {
        if (!isOfClass(parameterType, String.class)) {
          throw new IllegalParameterType("Parameter with @Author must be `String`");
        }

        return (record, invokeSessionContext) -> record.value().author;
      }

      if (annotation instanceof InnerProducerName) {
        producerName = (InnerProducerName) annotation;
      }

      if (annotation instanceof ToTopic) {
        toTopic = (ToTopic) annotation;
      }
    }

    if (isOfClass(parameterType, Box.class)) {
      return (record, invokeSessionContext) -> record.value();
    }

    if (isOfClass(parameterType, InnerProducerSender.class)) {
      if (producerName != null) {
        finalProducerName.set(producerName.value());
      }

      return new InnerProducerSenderValueReader(finalProducerName.get());
    }

    if (isOfClass(parameterType, InnerProducer.class)) {
      if (producerName != null) {
        finalProducerName.set(producerName.value());
      }

      if (toTopic == null) {
        throw new AbsentAnnotationToTopicOverInnerProducer();
      }

      return new InnerProducerValueReader(finalProducerName.get(), toTopic.value());
    }

    return new ParameterValueReader() {

      @Override
      public Object read(ConsumerRecord<byte[], Box> record, InvokeSessionContext invokeSessionContext) {
        return record.value().body;
      }

      @Override
      public Class<?> gettingBodyClass() {
        return extractClass(parameterType);
      }
    };
  }
}
