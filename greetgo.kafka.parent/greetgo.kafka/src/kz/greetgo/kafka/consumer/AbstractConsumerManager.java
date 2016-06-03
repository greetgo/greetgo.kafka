package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.util.ServerUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Method;
import java.util.*;

public abstract class AbstractConsumerManager {

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

  private final List<ConsumerThread> threadList = new ArrayList<>();

  protected abstract String cursorIdPrefix();

  protected abstract String topicPrefix();

  public void registerBean(Object bean) {
    for (Method method : bean.getClass().getMethods()) {
      Consume consume = ServerUtil.getAnnotation(method, Consume.class);
      if (consume != null) prepareThread(bean, method, consume);
    }
  }

  private final Map<String, Method> registeredConsumers = new HashMap<>();

  class ConsumerThread extends Thread {

    private final ConsumerDefinition consumerDefinition;
    boolean running = true;

    public ConsumerThread(ConsumerDefinition consumerDefinition) {
      this.consumerDefinition = consumerDefinition;
    }

    @Override
    public void run() {
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
          createProperties(notNull(cursorIdPrefix(), "cursorIdPrefix") + consumerDefinition.consume.cursorId()))
      ) {
        consumer.subscribe(addPrefix(notNull(topicPrefix(), "topicPrefix"),
            Arrays.asList(consumerDefinition.consume.topics())));

        final List<Box> list = new ArrayList<>();

        while (running) {
          list.clear();

          for (ConsumerRecord<String, String> record : consumer.poll(pollTimeout())) {
            list.add(strConverter().<Box>fromStr(record.value()));
          }

          if (list.size() == 0) continue;
          try {
            consumerDefinition.caller.call(list);
            consumer.commitSync();
          } catch (Exception e) {
            try {
              handleCallException(consumerDefinition.bean, consumerDefinition.method, e);
            } catch (Exception ex) {
              if (ex instanceof RuntimeException) throw (RuntimeException) ex;
              throw new RuntimeException(ex);
            }
          }
        }
      }
    }
  }

  private void prepareThread(final Object bean, final Method method, final Consume consume) {
    synchronized (registeredConsumers) {
      Method registeredMethod = registeredConsumers.get(consume.name());
      if (registeredMethod != null) {
        throw new RuntimeException("Consumer with name " + consume.name() + " already registered in " + registeredMethod
            + ". Secondary registration was in " + method);
      }
      registeredConsumers.put(consume.name(), method);
    }

    threadList.add(new ConsumerThread(new ConsumerDefinition(bean, method, consume)));
  }

  private static String notNull(String fieldValue, String fieldName) {
    if (fieldValue == null) throw new NullPointerException(fieldName);
    return fieldValue;
  }

  private static List<String> addPrefix(String prefix, List<String> list) {
    List<String> ret = new ArrayList<>(list.size());
    for (String str : list) {
      ret.add(prefix + str);
    }
    return ret;
  }

  protected abstract StrConverter strConverter();

  protected abstract void handleCallException(Object bean, Method method, Exception exception) throws Exception;

  protected long pollTimeout() {
    return 300;
  }

  public void startup() {
    for (ConsumerThread thread : threadList) {
      thread.start();
    }
  }

  public void shutdown() {
    for (ConsumerThread thread : threadList) {
      thread.running = false;
    }
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

  @SuppressWarnings("unused")
  public void shutdownAndJoin() {
    shutdown();
    join();
  }
}
