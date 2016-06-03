package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.util.ServerUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

  protected abstract String cursorIdPrefix();

  protected abstract String topicPrefix();

  private final Map<String, ConsumerDefinition> registeredBeans = new ConcurrentHashMap<>();

  public void registerBean(Object bean) {
    for (Method method : bean.getClass().getMethods()) {
      Consume consume = ServerUtil.getAnnotation(method, Consume.class);
      if (consume == null) continue;
      {
        ConsumerDefinition consumerDefinition = registeredBeans.get(consume.name());
        if (consumerDefinition != null) {
          throw new RuntimeException("Consumer with name " + consume.name() + " already registered in "
              + consumerDefinition.method + ". Secondary registration was in " + method);
        }
        registeredBeans.put(consume.name(), new ConsumerDefinition(bean, method, consume));
      }
    }
  }

  private final Map<ConsumerThread, ConsumerThread> threads = new ConcurrentHashMap<>();

  class ConsumerThread extends Thread {

    final ConsumerDefinition consumerDefinition;
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

        threads.remove(ConsumerThread.this);
      }
    }
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

  public void startAll() {
    for (String consumeName : registeredBeans.keySet()) {
      ensureStarted(consumeName);
    }
  }

  private boolean initiated = false;

  protected void initiate() throws Exception {
  }

  private void init() {
    try {
      if (initiated) return;
      initiate();
      initiated = true;
    } catch (Exception e) {
      if (e instanceof RuntimeException) throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }

  public synchronized void ensureStarted(String consumeName) {
    init();
    for (ConsumerThread thread : threads.keySet()) {
      if (thread.consumerDefinition.consume.name().equals(consumeName)) {
        if (thread.running) return;
      }
    }

    ConsumerDefinition consumerDefinition = registeredBeans.get(consumeName);
    if (consumerDefinition == null) throw new RuntimeException("No consumer " + consumeName);

    ConsumerThread thread = new ConsumerThread(consumerDefinition);
    thread.start();
    threads.put(thread, thread);
  }

  public synchronized void stop(String consumeName) {
    boolean was = false;

    for (ConsumerThread thread : threads.keySet()) {
      if (thread.consumerDefinition.consume.name().equals(consumeName)) {
        thread.running = false;
        was = true;
      }
    }

    if (was) return;

    throw new RuntimeException("No consumer with name = " + consumeName);
  }

  public void stopAll() {
    for (ConsumerThread thread : threads.keySet()) {
      thread.running = false;
    }
  }

  public void join() {
    for (Thread thread : threads.keySet()) {
      try {
        thread.join();
      } catch (InterruptedException ignore) {
      }
    }
  }

  @SuppressWarnings("unused")
  public void stopAllAndJoin() {
    stopAll();
    join();
  }
}
