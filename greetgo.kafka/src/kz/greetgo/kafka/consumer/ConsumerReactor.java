package kz.greetgo.kafka.consumer;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.serializer.BoxDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static kz.greetgo.kafka.core.logger.LoggerType.LOG_CONSUMER_FINISH_WORKER;
import static kz.greetgo.kafka.core.logger.LoggerType.LOG_CONSUMER_WAKEUP_EXCEPTION_HAPPENED;
import static kz.greetgo.kafka.core.logger.LoggerType.LOG_START_CONSUMER_WORKER;
import static kz.greetgo.kafka.core.logger.LoggerType.SHOW_CONSUMER_WORKER_CONFIG;

public class ConsumerReactor {

  //
  // Input values for reactor
  //

  public Logger logger;
  public Kryo kryo;
  public ConsumerDefinition consumerDefinition;
  public EventConfigStorage configStorage;
  public Supplier<String> bootstrapServers;
  public String storageRootPath;
  public String storageParentConfigPath;

  /**
   * Start reactor
   */
  public void start() {
    if (!working.get()) {
      throw new IllegalStateException("Cannot start closed ConsumerReactor");
    }

    consumerConfigWorker.setParentPath(storageParentConfigPath);

    {
      String configPath = consumerDefinition.getConfigPath();
      if (storageRootPath != null) {
        configPath = storageRootPath + "/" + configPath;
      }
      consumerConfigWorker.setConfigPath(configPath);
    }

    consumerConfigWorker.start();

    {
      StringBuilder sb = new StringBuilder();
      String folderPath = consumerDefinition.getFolderPath();
      if (folderPath != null) {
        sb.append(folderPath).append('/');
      }
      sb.append(consumerDefinition.getControllerClass().getSimpleName());
      sb.append(".txt");
      consumerConfigWorker.setConfigPath(sb.toString());
    }

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

  //
  // Working mechanism
  //

  private final ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, this::refresh);

  private final AtomicBoolean working = new AtomicBoolean(true);

  private void refresh() {
    Set<Long> toDelete = new HashSet<>();

    int currentCount = 0;

    for (Map.Entry<Long, Worker> e : workers.entrySet()) {
      if (!e.getValue().isRunning()) {
        toDelete.add(e.getKey());
        continue;
      }
      currentCount++;
    }

    toDelete.forEach(workers::remove);

    int workerCount = consumerConfigWorker.getWorkerCount();

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
        if (logger.isShow(LOG_START_CONSUMER_WORKER)) {
          logger.logConsumerStartWorker(consumerDefinition.logDisplay(), id);
        }

        Map<String, Object> configMap = consumerConfigWorker.getConfigMap();
        configMap.put("bootstrap.servers", bootstrapServers.get());
        configMap.put("auto.offset.reset", consumerDefinition.getAutoOffsetReset().name().toLowerCase());
        configMap.put("group.id", consumerDefinition.getGroupId());
        configMap.put("enable.auto.commit", consumerDefinition.isAutoCommit() ? "true" : "false");

        if (logger.isShow(SHOW_CONSUMER_WORKER_CONFIG)) {
          logger.logConsumerWorkerConfig(consumerDefinition.logDisplay(), id, configMap);
        }

        ByteArrayDeserializer forKey = new ByteArrayDeserializer();
        BoxDeserializer forValue = new BoxDeserializer(kryo);

        try (KafkaConsumer<byte[], Box> consumer = new KafkaConsumer<>(configMap, forKey, forValue)) {
          consumer.subscribe(consumerDefinition.topicList());
          while (working.get() && workers.containsKey(id)) {
            try {
              ConsumerRecords<byte[], Box> records = consumer.poll(consumerConfigWorker.pollDuration());
              if (consumerDefinition.invoke(records)) {
                consumer.commitSync();
              } else {
                workers.remove(id);
              }

            } catch (org.apache.kafka.common.errors.WakeupException wakeupException) {
              if (logger.isShow(LOG_CONSUMER_WAKEUP_EXCEPTION_HAPPENED)) {
                logger.logConsumerWakeupExceptionHappened(wakeupException);
              }
            }
          }
        }

      } finally {
        running.set(false);
        if (logger.isShow(LOG_CONSUMER_FINISH_WORKER)) {
          logger.logConsumerFinishWorker(consumerDefinition.logDisplay(), id);
        }
      }
    }
  }

}
