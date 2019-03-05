package kz.greetgo.kafka2.consumer;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.model.Box;
import kz.greetgo.kafka2.serializer.BoxDeserializer;
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

public class ConsumerReactor {

  //
  // Setting from output
  //

  public Kryo kryo;
  public ConsumerDefinition consumerDefinition;
  public ConfigStorage configStorage;
  public Supplier<String> bootstrapServers;
  public ConsumerLogger consumerLogger;

  public void start() {
    if (!working.get()) {
      throw new IllegalStateException("Cannot start closed ConsumerReactor");
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

  //
  // Working mechanism
  //

  private final ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, this::refresh);

  private final AtomicBoolean working = new AtomicBoolean(true);

  public void stop() {
    consumerConfigWorker.close();
    working.set(false);
  }

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
        consumerLogger.startWorker(consumerDefinition.logDisplay(), id);
        Map<String, Object> configMap = consumerConfigWorker.getConfigMap();
        configMap.put("bootstrap.servers", bootstrapServers.get());
        configMap.put("auto.offset.reset", consumerDefinition.getAutoOffsetReset());
        configMap.put("group.id", consumerDefinition.getGroupId());
        configMap.put("enable.auto.commit", consumerDefinition.isAutoCommit() ? "true" : "false");

        consumerLogger.showWorkerConfig(consumerDefinition.logDisplay(), id, configMap);

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
              consumerLogger.wakeupExceptionHappened(wakeupException);
            }
          }
        }

      } finally {
        running.set(false);
        consumerLogger.finishWorker(consumerDefinition.logDisplay(), id);
      }
    }
  }
}
