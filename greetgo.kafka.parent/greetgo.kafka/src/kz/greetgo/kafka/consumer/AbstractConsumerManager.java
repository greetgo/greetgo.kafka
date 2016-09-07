package kz.greetgo.kafka.consumer;

import kafka.consumer.*;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.events.KafkaEventCatcher;
import kz.greetgo.kafka.events.e.*;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.util.ServerUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.collection.Seq;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

public abstract class AbstractConsumerManager {

  protected abstract String bootstrapServers();

  protected abstract String zookeeperConnectStr();

  protected abstract String cursorIdPrefix();

  protected abstract String topicPrefix();

  protected abstract String soulId();

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

  public void setWorkingThreads(String consumeName, int threadCount) {
    init();
    final ConsumerDot consumerDot = registeredBeans.get(consumeName);
    if (consumerDot == null) throw new IllegalArgumentException("No consumer with name " + consumeName);

    consumerDot.setWorkingThreads(threadCount);


  }

  private final Map<String, ConsumerDot> registeredBeans = new ConcurrentHashMap<>();

  protected KafkaEventCatcher eventCatcher() {
    return null;
  }

  public void registerBean(Object bean) {
    for (Method method : bean.getClass().getMethods()) {
      Consume consume = ServerUtil.getAnnotation(method, Consume.class);
      if (consume == null) continue;

      {
        ConsumerDot consumerDot = registeredBeans.get(consume.name());
        if (consumerDot != null) {
          throw new RuntimeException("Consumer with name " + consume.name() + " already registered in "
              + consumerDot.consumerDefinition.method + ". Secondary registration is in " + method);
        }
      }

      {
        ConsumerDefinition consumerDefinition = new ConsumerDefinition(
            bean, method, consume, ServerUtil.getAnnotation(method, AddSoulIdToEndOfCursorId.class) != null
        );
        if (eventCatcher() != null && eventCatcher().needCatchOf(ConsumerEventRegister.class)) {
          eventCatcher().catchEvent(new ConsumerEventRegister(consumerDefinition));
        }
        registeredBeans.put(consume.name(), new ConsumerDot(consumerDefinition));
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

  protected String getCursorId(ConsumerDefinition consumerDefinition) {
    return notNull(cursorIdPrefix(), "cursorIdPrefix") + consumerDefinition.consume.cursorId() +
        (consumerDefinition.addSoulIdToEndOfCursorId ? '_' + notNull(soulId(), "soulId") : "");
  }

  @SuppressWarnings("UnusedParameters")
  protected void beforeCall(ConsumerDefinition consumerDefinition, int listSize) {
  }

  protected Properties createNewProperties(String groupId, ConsumerDefinition consumerDefinition) {
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

  protected Properties createOldProperties(String groupId, ConsumerDefinition consumerDefinition) {
    Properties pp = new Properties();
    pp.setProperty("auto.offset.reset", "smallest");
    pp.setProperty("group.id", groupId);
    pp.setProperty("auto.commit.enable", "false");
    pp.setProperty("zookeeper.connect", zookeeperConnectStr());
    return pp;
  }

  class ConsumerDot {
    final ConsumerDefinition consumerDefinition;
    final NewConsumerRunner newRunner;
    final OldConsumerRunner oldRunner;

    public ConsumerDot(ConsumerDefinition consumerDefinition) {
      this.consumerDefinition = consumerDefinition;
      newRunner = new NewConsumerRunner(consumerDefinition);
      oldRunner = new OldConsumerRunner(consumerDefinition);
    }

    public void setWorkingThreads(int threadCount) {
      if (threadCount >= 0) {
        oldRunner.setWorkingThreads(0);
        newRunner.setWorkingThreads(threadCount);
      } else {
        oldRunner.setWorkingThreads(-threadCount);
        newRunner.setWorkingThreads(0);
      }
    }

    public void join() {
      newRunner.join();
      oldRunner.join();
    }
  }

  private List<String> topicList(ConsumerDefinition consumerDefinition) {
    return addPrefix(
        notNull(topicPrefix(), "topicPrefix"),
        Arrays.asList(consumerDefinition.consume.topics())
    );
  }

  abstract class ConsumerRunner {

    final ConsumerDefinition consumerDefinition;

    public ConsumerRunner(ConsumerDefinition consumerDefinition) {
      this.consumerDefinition = consumerDefinition;
    }

    public abstract void join();
  }

  class NewConsumerRunner extends ConsumerRunner {

    public NewConsumerRunner(ConsumerDefinition consumerDefinition) {
      super(consumerDefinition);
    }

    final Map<NewConsumerThread, NewConsumerThread> threads = new ConcurrentHashMap<>();

    public synchronized void setWorkingThreads(int threadCount) {

      int currentCount = 0;

      for (NewConsumerThread thread : threads.values()) {
        if (thread.isRunning()) currentCount++;
      }

      if (threadCount == currentCount) return;

      if (threadCount < currentCount) {
        for (NewConsumerThread thread : threads.keySet()) {
          thread.shutdown();
          if (--currentCount <= threadCount) return;
        }
        return;
      }

      for (int i = 0, n = threadCount - currentCount; i < n; i++) {
        NewConsumerThread thread = new NewConsumerThread();
        thread.start();
        threads.put(thread, thread);
      }

    }

    @Override
    public void join() {
      threads.values().forEach(t -> {
        try {
          t.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
    }

    class NewConsumerThread extends Thread {
      final AtomicBoolean running = new AtomicBoolean(true);

      boolean isRunning() {
        return running.get();
      }

      void shutdown() {
        running.set(false);
      }

      public NewConsumerThread() {
        setName(consumerDefinition.consume.name() + "_NEW_" + getName());
      }

      @Override
      public void run() {
        String cursorId = getCursorId(consumerDefinition);

        List<String> topicList = topicList(consumerDefinition);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createNewProperties(cursorId, consumerDefinition))) {
          consumer.subscribe(topicList);

          if (eventCatcher() != null && eventCatcher().needCatchOf(NewConsumerEventStart.class)) {
            eventCatcher().catchEvent(new NewConsumerEventStart(consumerDefinition, cursorId, topicList));
          }

          final List<Box> list = new ArrayList<>();

          while (running.get()) {
            list.clear();

            for (ConsumerRecord<String, String> record : consumer.poll(pollTimeout())) {
              list.add(strConverter().<Box>fromStr(record.value()));
            }

            final int listSize = list.size();

            if (listSize == 0) continue;

            try {
              beforeCall(consumerDefinition, listSize);
              consumerDefinition.caller.call(unmodifiableList(list));
              consumer.commitSync();
            } catch (Exception e) {
              if (eventCatcher() != null && eventCatcher().needCatchOf(NewConsumerEventException.class)) {
                eventCatcher().catchEvent(new NewConsumerEventException(consumerDefinition, cursorId, topicList, e));
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
          threads.remove(NewConsumerThread.this);
          if (eventCatcher() != null && eventCatcher().needCatchOf(NewConsumerEventStop.class)) {
            eventCatcher().catchEvent(new NewConsumerEventStop(consumerDefinition, cursorId, topicList));
          }
        }
      }
    }

  }

  class OldConsumerRunner extends ConsumerRunner {
    public OldConsumerRunner(ConsumerDefinition consumerDefinition) {
      super(consumerDefinition);
    }

    OldConsumerRunnerOnce onceRunner = new OldConsumerRunnerOnce(0);
    final AtomicInteger nextThreadIndex = new AtomicInteger(1);

    public synchronized void setWorkingThreads(int threadCount) {
      if (onceRunner.threadCount == threadCount) return;
      onceRunner.shutdown();
      onceRunner = new OldConsumerRunnerOnce(threadCount);
    }

    @Override
    public void join() {
      onceRunner.join();
    }

    class OldConsumerRunnerOnce {
      final int threadCount;
      final ConsumerConnector consumerConnector;
      final List<Thread> threadList = new ArrayList<>();

      public OldConsumerRunnerOnce(int threadCount) {
        this.threadCount = threadCount;
        if (threadCount <= 0) {
          consumerConnector = null;
          return;
        }

        String cursorId = getCursorId(consumerDefinition);

        final ConsumerConfig consumerConfig = new ConsumerConfig(createOldProperties(cursorId, consumerDefinition));
        consumerConnector = Consumer$.MODULE$.create(consumerConfig);

        final List<String> topicList = topicList(consumerDefinition);
        final String whileListStr = topicList.stream().collect(Collectors.joining("|"));
        final Whitelist filter = new Whitelist(whileListStr);

        final DefaultDecoder keyDecoder = new DefaultDecoder(null);
        final DefaultDecoder valueDecoder = new DefaultDecoder(null);

        final Seq<KafkaStream<byte[], byte[]>> kafkaStream = consumerConnector.createMessageStreamsByFilter(
            filter, threadCount, keyDecoder, valueDecoder
        );

        final scala.collection.Iterator<KafkaStream<byte[], byte[]>> kafkaStreamIterator = kafkaStream.iterator();

        final StringDeserializer stringDeserializer = new StringDeserializer();

        while (kafkaStreamIterator.hasNext()) {

          final KafkaStream<byte[], byte[]> threadKafkaStream = kafkaStreamIterator.next();

          final int threadIndex = nextThreadIndex.incrementAndGet();

          Thread thread = new Thread(() -> {

            final ConsumerIterator<byte[], byte[]> iterator = threadKafkaStream.iterator();

            while (true) {

              final MessageAndMetadata<byte[], byte[]> mam;
              try {
                mam = iterator.next();
              } catch (NoSuchElementException e) {
                if (eventCatcher() != null && eventCatcher().needCatchOf(OldConsumerEventStop.class)) {
                  eventCatcher().catchEvent(new OldConsumerEventStop(consumerDefinition, cursorId, topicList));
                }
                break;
              }

              final String message = stringDeserializer.deserialize(whileListStr, mam.message());
              final Box box = strConverter().<Box>fromStr(message);
              final List<Box> boxList = Collections.singletonList(box);

              try {
                beforeCall(consumerDefinition, 1);
                consumerDefinition.caller.call(boxList);
                consumerConnector.commitOffsets(true);
              } catch (Exception e) {
                if (eventCatcher() != null && eventCatcher().needCatchOf(OldConsumerEventException.class)) {
                  eventCatcher().catchEvent(new OldConsumerEventException(consumerDefinition, cursorId, topicList, e));
                }
                try {
                  handleCallException(consumerDefinition, e);
                } catch (Exception ex) {
                  if (ex instanceof RuntimeException) throw (RuntimeException) ex;
                  throw new RuntimeException(ex);
                }
              }

            }

          });

          thread.setName(consumerDefinition.consume.name() + "_OLD_" + threadIndex);
          threadList.add(thread);
          thread.start();
        }
      }

      public void shutdown() {
        final ConsumerConnector consumerConnector = this.consumerConnector;
        if (consumerConnector != null) {
          consumerConnector.shutdown();
        }
      }

      public void join() {
        threadList.forEach(t -> {
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
      }
    }
  }

  public void stopAll() {
    registeredBeans.values().forEach(b -> b.setWorkingThreads(0));
  }

  public void joinAll() {
    registeredBeans.values().forEach(ConsumerDot::join);
  }

  public void stopAllAndJoin() {
    stopAll();
    joinAll();
  }

}
