package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.Head;
import kz.greetgo.kafka.str.StrConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class AbstractConsumerManager {
  public StrConverter strConverter;
  private boolean running = true;

  protected abstract String bootstrapServers();

  protected Properties createProperties(String groupId) {
    final Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers());
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("auto.offset.reset", "earliest");
    return props;
  }

  private final List<Thread> threadList = new ArrayList<>();

  public void appendBean(Object bean) {
    for (Method method : bean.getClass().getMethods()) {
      Consume consume = method.getAnnotation(Consume.class);
      if (consume != null) prepareThread(bean, method, consume);
    }
  }

  private void prepareThread(final Object bean, final Method method, final Consume consume) {
    threadList.add(new Thread(new Runnable() {
      final Caller caller = createCaller(bean, method);

      @Override
      public void run() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createProperties(consume.groupId()))) {
          consumer.subscribe(Arrays.asList(consume.topics()));

          final List<Box> list = new ArrayList<>();

          while (running) {
            list.clear();
            System.out.println("hrewr: in circle of method " + method.getName());
            for (ConsumerRecord<String, String> record : consumer.poll(pollTimeout())) {
              list.add(strConverter.<Box>fromStr(record.value()));
            }
            if (list.size() == 0) continue;
            try {
              caller.call(list);
              consumer.commitSync();
            } catch (Exception e) {
              handleCallException(bean, method, e);
            }
          }

        }
      }
    }));
  }

  protected abstract void handleCallException(Object bean, Method method, Exception exception);

  protected long pollTimeout() {
    return 100;
  }

  public interface Caller {
    void call(List<Box> list);
  }

  static Caller createCaller(final Object bean, final Method method) {
    Type[] parameterTypes = method.getGenericParameterTypes();

    if (parameterTypes.length == 1 && isListOfBoxes(parameterTypes[0])) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          try {
            method.invoke(bean, list);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    if (parameterTypes.length == 1 && isList(parameterTypes[0])) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          List<Object> objectList = new ArrayList<>(list.size());
          for (Box box : list) {
            objectList.add(box.body);
          }
          try {
            method.invoke(bean, objectList);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    if (parameterTypes.length == 1 && parameterTypes[0] == Box.class) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          for (Box box : list) {
            try {
              method.invoke(bean, box);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    if (parameterTypes.length == 1) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          for (Box box : list) {
            try {
              method.invoke(bean, box.body);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    if (parameterTypes.length == 2 && parameterTypes[1] == Head.class) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          for (Box box : list) {
            try {
              method.invoke(bean, box.body, box.head);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    throw new RuntimeException("Cannot create caller for " + method.toGenericString() + "\n" +
        "You can use following variants:\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(List<Box> list)...\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(List<SomeClass> list)...\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(Box box) ...\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(SomeClass asd) ...\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(SomeClass asd, Head head) ...\n" +
        "* Box - it is " + Box.class.getName() + "\n" +
        "* Head - it is " + Head.class.getName() + "\n" +
        "* SomeClass - it is some class except Box or Head");
  }

  private static boolean isListOfBoxes(Type type) {
    if (!(type instanceof ParameterizedType)) return false;
    ParameterizedType parameterizedType = (ParameterizedType) type;
    if (parameterizedType.getRawType() != List.class) return false;
    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
    if (actualTypeArguments.length != 1) return false;
    return actualTypeArguments[0] == Box.class;
  }

  private static boolean isList(Type type) {
    if (type == List.class) return true;
    if (!(type instanceof ParameterizedType)) return false;
    ParameterizedType parameterizedType = (ParameterizedType) type;
    return parameterizedType.getRawType() == List.class;
  }

  public void startup() {
    for (Thread thread : threadList) {
      thread.start();
    }
  }

  public void shutdown() {
    running = false;
  }

  public void join() {
    for (Thread thread : threadList) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        //ignore
      }
    }
  }

  public void shutdownAndJoin() {
    shutdown();
    join();
  }

}
