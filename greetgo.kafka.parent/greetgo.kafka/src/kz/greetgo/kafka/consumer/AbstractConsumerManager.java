package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.events.KafkaEventCatcher;
import kz.greetgo.kafka.events.e.ConsumerEventException;
import kz.greetgo.kafka.events.e.ConsumerEventRegister;
import kz.greetgo.kafka.events.e.ConsumerEventStart;
import kz.greetgo.kafka.events.e.ConsumerEventStop;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.util.ServerUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

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

  protected KafkaEventCatcher eventCatcher() {
    return null;
  }

  public void registerBean(Object bean) {
    for (Method method : bean.getClass().getMethods()) {
      Consume consume = ServerUtil.getAnnotation(method, Consume.class);
      if (consume == null) continue;

      {
        ConsumerDefinition consumerDefinition = registeredBeans.get(consume.name());
        if (consumerDefinition != null) {
          throw new RuntimeException("Consumer with name " + consume.name() + " already registered in "
              + consumerDefinition.method + ". Secondary registration is in " + method);
        }
      }

      {
        ConsumerDefinition consumerDefinition = new ConsumerDefinition(bean, method, consume);
        if (eventCatcher()!=null&&eventCatcher().needCatchOf(ConsumerEventRegister.class)) {
          eventCatcher().catchEvent(new ConsumerEventRegister(consumerDefinition));
        }
        registeredBeans.put(consume.name(), consumerDefinition);
      }
    }
  }

  private final Map<ConsumerThread, ConsumerThread> threads = new ConcurrentHashMap<>();

  class ConsumerThread extends Thread {

    final ConsumerDefinition consumerDefinition;
    final AtomicBoolean running = new AtomicBoolean(true);

    boolean running() {
      return running.get();
    }

    void shutdown() {
      running.set(false);
    }

    public ConsumerThread(ConsumerDefinition consumerDefinition) {
      this.consumerDefinition = consumerDefinition;
    }

    @Override
    public void run() {
      String cursorId = notNull(cursorIdPrefix(), "cursorIdPrefix") + consumerDefinition.consume.cursorId();

      List<String> topicPrefixList = addPrefix(
          notNull(topicPrefix(), "topicPrefix"),
          Arrays.asList(consumerDefinition.consume.topics())
      );

      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createProperties(cursorId))) {
        consumer.subscribe(topicPrefixList);

        if (eventCatcher() != null && eventCatcher().needCatchOf(ConsumerEventStart.class)) {
          eventCatcher().catchEvent(new ConsumerEventStart(consumerDefinition, cursorId, topicPrefixList));
        }

        final List<Box> list = new ArrayList<>();

        while (running.get()) {
          list.clear();

          for (ConsumerRecord<String, String> record : consumer.poll(pollTimeout())) {
            list.add(strConverter().<Box>fromStr(record.value()));
          }

          if (list.size() == 0) continue;
          try {
            consumerDefinition.caller.call(unmodifiableList(list));
            consumer.commitSync();
          } catch (Exception e) {
            if (eventCatcher() != null && eventCatcher().needCatchOf(ConsumerEventException.class)) {
              eventCatcher().catchEvent(new ConsumerEventException(consumerDefinition, cursorId, topicPrefixList, e));
            }
            try {
              handleCallException(consumerDefinition, e);
            } catch (Exception ex) {
              if (ex instanceof RuntimeException) throw (RuntimeException) ex;
              throw new RuntimeException(ex);
            }
          }
        }

      } finally {
        threads.remove(ConsumerThread.this);
        if (eventCatcher() != null && eventCatcher().needCatchOf(ConsumerEventStop.class)) {
          eventCatcher().catchEvent(new ConsumerEventStop(consumerDefinition, cursorId, topicPrefixList));
        }
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

  protected abstract void handleCallException(ConsumerDefinition consumerDefinition, Exception exception) throws Exception;

  protected long pollTimeout() {
    return 300;
  }

  public Set<String> consumerNames() {
    init();
    return unmodifiableSet(registeredBeans.keySet());
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
        if (thread.running()) return;
      }
    }

    ConsumerDefinition consumerDefinition = registeredBeans.get(consumeName);
    if (consumerDefinition == null) throw new RuntimeException("No consumer " + consumeName);

    ConsumerThread thread = new ConsumerThread(consumerDefinition);
    thread.start();
    threads.put(thread, thread);
  }

  @SuppressWarnings("unused")
  public synchronized void stop(String consumeName) {
    for (ConsumerThread thread : threads.keySet()) {
      if (thread.consumerDefinition.consume.name().equals(consumeName)) {
        thread.shutdown();
      }
    }
  }

  public void stopAll() {
    for (ConsumerThread thread : threads.keySet()) {
      thread.shutdown();
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
