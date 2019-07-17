package kz.greetgo.kafka.massive.tests;

import kz.greetgo.kafka.consumer.ConsumerConfigDefaults;
import kz.greetgo.kafka.consumer.annotations.ConsumerName;
import kz.greetgo.kafka.consumer.annotations.GroupId;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.core.KafkaReactorImpl;
import kz.greetgo.kafka.core.config.EventConfigStorageZooKeeper;
import kz.greetgo.kafka.core.logger.LoggerType;
import kz.greetgo.kafka.massive.tests.model.Client;
import kz.greetgo.kafka.producer.KafkaFuture;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.kafka.util.ConfigLines;
import kz.greetgo.util.RND;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static kz.greetgo.kafka.massive.tests.TimeUtil.nanosRead;

public class MassiveTestServer {

  public static class Consumers {
    private final Path workingDir;

    public Consumers(Path workingDir) {
      this.workingDir = workingDir;
    }

    private final ConcurrentHashMap<String, String> errors = new ConcurrentHashMap<>();

    @Topic("CLIENT")
    @ConsumerName("CLIENT")
    @GroupId("asd")
    public void readClient(Client client) {
      if ("ok".equals(client.name)) {
        printClient(client);
        return;
      }

      if (errors.containsKey(client.id)) {
        printClient(client);
        return;
      }

      errors.put(client.id, "1");

      System.out.println("rv35hvg345 ERROR THROWS");
      throw new RuntimeException("ERROR");

    }

    private void printClient(Client client) {
      Path come = workingDir.resolve("come");
      come.toFile().mkdirs();
      Path file = come.resolve(client.id);
      if (!Files.exists(file)) {
        createFile(file);
      } else {
        for (int i = 2; ; i++) {
          Path f = come.resolve(client.id + "-" + i);
          if (!Files.exists(f)) {
            createFile(f);
            break;
          }
        }
      }

      System.out.println("Come client " + client + " from " + Thread.currentThread().getName());
    }
  }

  private static void createFile(Path pathToFile) {
    try {
      pathToFile.toFile().createNewFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    Path pwd = new File(".").getAbsoluteFile().toPath().normalize();

    Path workingDir = pwd.resolve("build").resolve("MassiveTestServer");

    Files.createDirectories(workingDir);

    Path workingFile = workingDir.resolve("working.file");
    Path clientExistsFile = workingDir.resolve("exists").resolve("client-exists.file");
    Files.createDirectories(clientExistsFile.resolve("..").normalize());

    workingFile.toFile().createNewFile();

    String kafkaServers = "localhost:9091,localhost:9092,localhost:9093,localhost:9094";
    String zookeeperServers = "localhost:2181,localhost:2182,localhost:2183";

    Map<String, Object> conf = new HashMap<>();
    conf.put("bootstrap.servers", kafkaServers);

    try (AdminClient adminClient = KafkaAdminClient.create(conf)) {

      if (!clientExistsFile.toFile().exists()) {
        NewTopic newTopic = new NewTopic("CLIENT", 480, (short) 2);

        adminClient.createTopics(singletonList(newTopic)).all();

        clientExistsFile.toFile().createNewFile();
      }

    }

    Consumers consumers = new Consumers(workingDir);

    KafkaReactorImpl reactor = createReactor(kafkaServers, zookeeperServers);

    reactor.addController(consumers);

    System.out.println("Before start consumers");

    reactor.startConsumers();
    try {

      System.out.println("Started consumers");

      ProducerFacade mainProducer = reactor.createProducer("main");

      System.out.println("Start waiting process");

      Path insertClientPortionFile = workingDir.resolve("insert-client-portion");

      Path insertClientPortionFileParallel = workingDir.resolve("insert-client-portion-parallel");

      insertClientPortionFile.toFile().createNewFile();
      insertClientPortionFileParallel.toFile().createNewFile();

      Path setPortion300 = workingDir.resolve("setPortion300");
      Path setPortion3000 = workingDir.resolve("setPortion3000");
      Path setPortion30000 = workingDir.resolve("setPortion30000");

      setPortion300.toFile().createNewFile();
      setPortion3000.toFile().createNewFile();
      setPortion30000.toFile().createNewFile();

      AtomicInteger portion = new AtomicInteger(300);

      while (workingFile.toFile().exists()) {

        setPortion(setPortion300, portion, 300);
        setPortion(setPortion3000, portion, 3000);
        setPortion(setPortion30000, portion, 30000);

        if (!insertClientPortionFile.toFile().exists()) {
          insertClientPortionFile.toFile().createNewFile();

          String id = RND.str(3);

          long started = System.nanoTime();
          int count = portion.get();
          for (int i = 0; i < count; i++) {
            Client client = new Client();
            client.id = id + "-" + i;
            client.surname = RND.str(10);
            client.name = i == 10 ? "err" : "ok";

            mainProducer
              .sending(client)
              .toTopic("CLIENT")
              .go()
              .awaitAndGet();
          }
          System.out.println("g5v43gh2v5 :: Inserted for " + nanosRead(System.nanoTime() - started));

        }

        if (!insertClientPortionFileParallel.toFile().exists()) {
          insertClientPortionFileParallel.toFile().createNewFile();

          List<KafkaFuture> futures = new ArrayList<>();

          String id = RND.str(3);

          int count = portion.get();
          long started = System.nanoTime();
          for (int i = 0; i < count; i++) {
            Client client = new Client();
            client.id = id + "-" + i;
            client.surname = RND.str(10);
            client.name = i == 10 ? "err" : "ok";

            futures.add(mainProducer
              .sending(client)
              .toTopic("CLIENT")
              .go());
          }
          long middle = System.nanoTime();

          futures.forEach(KafkaFuture::awaitAndGet);

          long end = System.nanoTime();

          System.out.println("5hb4326gv :: Inserted for " + nanosRead(end - started)
            + " : middle for " + nanosRead(middle - started));

        }

        Thread.sleep(700);

      }

    } finally {
      System.out.println("Stopping reactor");
      reactor.stopConsumers();
    }

    System.out.println("Finished");

  }

  private static void setPortion(Path setPortion, AtomicInteger portionBack, int portion) {
    if (setPortion.toFile().exists()) {
      return;
    }
    createFile(setPortion);
    portionBack.set(portion);
    System.out.println("v34g5gv75 :: set portion to " + portion);
  }

  private static KafkaReactorImpl createReactor(String kafkaServers, String zookeeperServers) {
    KafkaReactorImpl reactor = new KafkaReactorImpl() {
      @Override
      protected void putProducerDefaultValues(ConfigLines configLines) {
        configLines.putValue("prod.acts                    ", "all");
        configLines.putValue("prod.buffer.memory           ", "33554432");
        configLines.putValue("prod.batch.size              ", "16384");
        configLines.putValue("prod.compression.type        ", "lz4");
        configLines.putValue("prod.request.timeout.ms      ", "30000");
        configLines.putValue("prod.connections.max.idle.ms ", "540000");
        configLines.putValue("prod.linger.ms               ", "1");
        configLines.putValue("prod.batch.size              ", "16384");

        configLines.putValue("prod.retries                               ", "2147483647");
        configLines.putValue("prod.max.in.flight.requests.per.connection ", "1");
        configLines.putValue("prod.delivery.timeout.ms                   ", "35000");
      }
    };

    ConsumerConfigDefaults ccd = new ConsumerConfigDefaults();

    ccd.addDefinition(" Long   con.auto.commit.interval.ms           1000  ");
    ccd.addDefinition(" Long   con.fetch.min.bytes                      1  ");
    ccd.addDefinition(" Long   con.max.partition.fetch.bytes      1048576  ");
    ccd.addDefinition(" Long   con.connections.max.idle.ms         540000  ");
    ccd.addDefinition(" Long   con.default.api.timeout.ms           60000  ");
    ccd.addDefinition(" Long   con.fetch.max.bytes               52428800  ");

    ccd.addDefinition(" Long   con.session.timeout.ms               10000  ");
    ccd.addDefinition(" Long   con.heartbeat.interval.ms             3000  ");
    ccd.addDefinition(" Long   con.max.poll.interval.ms           3000000  ");
    ccd.addDefinition(" Long   con.max.poll.records                   500  ");

    ccd.addDefinition(" Long   con.receive.buffer.bytes             65536  ");
    ccd.addDefinition(" Long   con.request.timeout.ms               30000  ");
    ccd.addDefinition(" Long   con.send.buffer.bytes               131072  ");
    ccd.addDefinition(" Long   con.fetch.max.wait.ms                  500  ");

    ccd.addDefinition(" Int out.worker.count        1  ");
    ccd.addDefinition(" Int out.poll.duration.ms  800  ");

    reactor.consumerConfigDefaults = ccd;

    reactor.logger().setDestination(new SimplePrinter());
    reactor.logger().setShowLogger(LoggerType.SHOW_CONSUMER_WORKER_CONFIG, true);
    reactor.logger().setShowLogger(LoggerType.SHOW_PRODUCER_CONFIG, true);
    reactor.logger().setShowLogger(LoggerType.LOG_CLOSE_PRODUCER, true);
    reactor.logger().setShowLogger(LoggerType.LOG_CONSUMER_ERROR_IN_METHOD, true);
    reactor.logger().setShowLogger(LoggerType.LOG_CONSUMER_ILLEGAL_ACCESS_EXCEPTION_INVOKING_METHOD, true);
    reactor.logger().setShowLogger(LoggerType.LOG_CONSUMER_REACTOR_REFRESH, true);
    reactor.logger().setShowLogger(LoggerType.LOG_CONSUMER_FINISH_WORKER, true);
    reactor.logger().setShowLogger(LoggerType.LOG_CONSUMER_POLL_EXCEPTION_HAPPENED, true);
    reactor.logger().setShowLogger(LoggerType.LOG_CONSUMER_COMMIT_SYNC_EXCEPTION_HAPPENED, true);

    ModelRegistrar.registrar(reactor);

    reactor.setAuthorSupplier(() -> "pompei");
    reactor.setHostId("super-host");

    reactor.setBootstrapServers(() -> kafkaServers);


    EventConfigStorageZooKeeper consumerConfigStorage = new EventConfigStorageZooKeeper(
      "aaa/consumers", () -> zookeeperServers, () -> 30000
    );
    EventConfigStorageZooKeeper producerConfigStorage = new EventConfigStorageZooKeeper(
      "aaa/producers", () -> zookeeperServers, () -> 30000
    );

    reactor.setConsumerConfigStorage(consumerConfigStorage);
    reactor.setProducerConfigStorage(producerConfigStorage);


    return reactor;
  }
}
