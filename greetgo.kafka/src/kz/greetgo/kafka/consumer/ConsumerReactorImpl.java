package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.ConsumerPortionInvoking;
import kz.greetgo.kafka.core.ProducerSynchronizer;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.serializer.BoxDeserializer;
import kz.greetgo.strconverter.StrConverter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static kz.greetgo.kafka.core.logger.LoggerType.LOG_CONSUMER_COMMIT_SYNC_EXCEPTION_HAPPENED;
import static kz.greetgo.kafka.core.logger.LoggerType.LOG_CONSUMER_FINISH_WORKER;
import static kz.greetgo.kafka.core.logger.LoggerType.LOG_CONSUMER_POLL_EXCEPTION_HAPPENED;
import static kz.greetgo.kafka.core.logger.LoggerType.LOG_CONSUMER_REACTOR_REFRESH;
import static kz.greetgo.kafka.core.logger.LoggerType.LOG_START_CONSUMER_WORKER;
import static kz.greetgo.kafka.core.logger.LoggerType.SHOW_CONSUMER_WORKER_CONFIG;

public class ConsumerReactorImpl implements ConsumerReactor {

  //
  // Input values for reactor
  //

  public Logger logger;
  public ProducerSynchronizer producerSynchronizer;
  public Supplier<StrConverter> strConverterSupplier;
  public ConsumerDefinition consumerDefinition;
  public EventConfigStorage configStorage;
  public Supplier<String> bootstrapServers;
  public String hostId;
  public Supplier<ConsumerConfigDefaults> consumerConfigDefaults;

  /**
   * Start reactor
   */
  public void start() {
    if (!working.get()) {
      throw new IllegalStateException("Cannot start closed ConsumerReactor");
    }

    consumerConfigWorker.setConfigPathPrefix(consumerDefinition.getConfigPath());
    consumerConfigWorker.setHostId(hostId);

    consumerConfigWorker.start();

    refresh();
  }

  /**
   * Stops reactor
   */
  public void stop() {
    if (working.get()) {
      if (working.compareAndSet(true, false)) {
        consumerConfigWorker.close();
      }
    }
  }

  /**
   * Waits for all consumers to finish their work.
   * <p>
   * You can call this method after called {@link #stop()} to wait for all consumer finished their work.
   */
  public void join() {

    while (true) {
      Iterator<Worker> iterator = workers.values().iterator();
      if (!iterator.hasNext()) {
        return;
      }
      try {
        iterator.next().join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

  }

  //
  // Working mechanism
  //

  private final ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(
    () -> configStorage,
    this::refresh,
    () -> consumerConfigDefaults.get()
  );

  private final AtomicBoolean working = new AtomicBoolean(true);

  @Override
  public void refresh() {
    Set<Long> toDelete = new HashSet<>();

    int currentCount = 0;

    for (Map.Entry<Long, Worker> e : workers.entrySet()) {
      if (!e.getValue().isRunning()) {
        toDelete.add(e.getKey());
      } else {
        currentCount++;
      }
    }

    toDelete.forEach(workers::remove);

    int workerCount = consumerConfigWorker.getWorkerCount();

    if (logger.isShow(LOG_CONSUMER_REACTOR_REFRESH)) {
      logger.logConsumerReactorRefresh(consumerDefinition, currentCount, workerCount);
    }

    if (workerCount > currentCount) {
      for (int i = 0, n = workerCount - currentCount; i < n; i++) {
        appendWorker();
      }
    } else if (workerCount < currentCount) {
      removeWorkers(currentCount - workerCount);
    }
  }


  private void appendWorker() {
    Worker worker = new Worker();
    workers.put(worker.id, worker);
    worker.start();
  }


  private void removeWorkers(int countToRemove) {
    int removed = 0;
    while (removed < countToRemove) {
      List<Long> list = new ArrayList<>(workers.keySet());

      if (list.isEmpty()) {
        return;
      }

      for (Long id : list) {
        Worker worker = workers.remove(id);
        if (worker != null && worker.isRunning()) {
          removed++;
          if (removed >= countToRemove) {
            return;
          }
        }
      }
    }
  }

  private final ConcurrentHashMap<Long, Worker> workers = new ConcurrentHashMap<>();

  private final AtomicLong nextValue = new AtomicLong(1);

  private class Worker extends Thread {
    private final long id = nextValue.getAndIncrement();

    private final AtomicBoolean running = new AtomicBoolean(true);

    public boolean isRunning() {
      return running.get();
    }

    @Override
    public void run() {
      try {

        Thread.currentThread().setName("kafka-consumer-" + consumerDefinition.logDisplay() + "-" + id);

        if (logger.isShow(LOG_START_CONSUMER_WORKER)) {
          logger.logConsumerStartWorker(consumerDefinition, id);
        }

        Map<String, Object> configMap = consumerConfigWorker.getConfigMap();
        configMap.put("bootstrap.servers", bootstrapServers.get());
        configMap.put("auto.offset.reset", consumerDefinition.getAutoOffsetReset().name().toLowerCase());
        configMap.put("group.id", consumerDefinition.getGroupId());
        configMap.put("enable.auto.commit", consumerDefinition.isAutoCommit() ? "true" : "false");

        //TODO pompei убрать надо или вынести в конфиг
        configMap.put("internal.leave.group.on.close", "false");

        if (logger.isShow(SHOW_CONSUMER_WORKER_CONFIG)) {
          logger.logConsumerWorkerConfig(consumerDefinition, id, configMap);
        }

        ByteArrayDeserializer forKey = new ByteArrayDeserializer();
        BoxDeserializer forValue = new BoxDeserializer(strConverterSupplier.get());

        Invoker invoker = consumerDefinition.getInvoker();

        try (Invoker.InvokeSession invokeSession = invoker.createSession()) {

          OUT:
          while (working.get() && workers.containsKey(id)) {

            try (KafkaConsumer<byte[], Box> consumer = new KafkaConsumer<>(configMap, forKey, forValue)) {
              consumer.subscribe(consumerDefinition.topicList());
              INNER:
              while (working.get() && workers.containsKey(id)) {

                final ConsumerRecords<byte[], Box> records;

                try {
                  records = consumer.poll(consumerConfigWorker.pollDuration());

                  if (records.count() == 0) {
                    continue INNER;
                  }

                } catch (RuntimeException exception) {
                  if (logger.isShow(LOG_CONSUMER_POLL_EXCEPTION_HAPPENED)) {
                    logger.logConsumerPollExceptionHappened(exception, consumerDefinition);
                  }
                  working.set(false);
                  continue OUT;
                }

                try (ConsumerPortionInvoking ignore = producerSynchronizer.startConsumerPortionInvoking()) {
                  if (!invokeSession.invoke(records).needToCommit()) {
                    continue OUT;
                  }
                }

                try {
                  consumer.commitSync();
                } catch (RuntimeException exception) {
                  if (logger.isShow(LOG_CONSUMER_COMMIT_SYNC_EXCEPTION_HAPPENED)) {
                    logger.logConsumerCommitSyncExceptionHappened(exception, consumerDefinition);
                  }
                  continue OUT;
                }


              }
            }

          }

        }

      } finally {

        running.set(false);
        workers.remove(id);

        if (logger.isShow(LOG_CONSUMER_FINISH_WORKER)) {
          logger.logConsumerFinishWorker(consumerDefinition, id);
        }

      }
    }
  }

}
