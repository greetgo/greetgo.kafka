package kz.greetgo.kafka.massive.tests;

import kz.greetgo.kafka.consumer.ConsumerConfigDefaults;
import kz.greetgo.kafka.consumer.InnerProducer;
import kz.greetgo.kafka.consumer.annotations.ConsumerName;
import kz.greetgo.kafka.consumer.annotations.GroupId;
import kz.greetgo.kafka.consumer.annotations.ToTopic;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.core.KafkaReactorImpl;
import kz.greetgo.kafka.core.config.EventConfigStorageZooKeeper;
import kz.greetgo.kafka.core.logger.LoggerType;
import kz.greetgo.kafka.massive.tests.model.Client;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.kafka.util.ConfigLines;
import kz.greetgo.util.RND;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class MassiveTestServer {
  private static ConcurrentHashMap<String, AtomicLong> readClientRuns = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, AtomicLong> readClientOutRuns = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, AtomicLong> readClientOut2Runs = new ConcurrentHashMap<>();

  private static final AtomicBoolean printClientToStdout = new AtomicBoolean(false);
  private static final AtomicBoolean generateErrors = new AtomicBoolean(false);

  static DataSource dataSource;

  static {
    BasicDataSource pool = new BasicDataSource();

    pool.setDriverClassName("org.postgresql.Driver");
    pool.setUrl("jdbc:postgresql://localhost:5432/kafka_test");
    pool.setUsername("kafka");
    pool.setPassword("111");

    pool.setInitialSize(0);

    dataSource = pool;
  }

  public static class Consumers {

    private final ConcurrentHashMap<String, String> errors = new ConcurrentHashMap<>();

    @GroupId("asd-1")
    @Topic("CLIENT")
    @ConsumerName("CLIENT")
    public void readClient(Client client,
                           @ToTopic("CLIENT-OUT")
                             InnerProducer<Client> clientProducer) {

      increment(readClientRuns, new SimpleDateFormat("HH:mm:ss").format(new Date()));

      if ("ok".equals(client.name)) {
        client.name = RND.str(10);
        clientProducer.send(client);
        return;
      }

      if (!generateErrors.get() || errors.containsKey(client.id)) {
        client.name = RND.str(10);
        clientProducer.send(client);
        return;
      }

      errors.put(client.id, "1");

      System.out.println("rv35hvg345 :: ERROR THROWS");
      throw new RuntimeException("ERROR");

    }

    final AtomicLong sleepClientOut = new AtomicLong(0);
    final AtomicLong sleepClientOut2 = new AtomicLong(0);

    @Topic("CLIENT-OUT")
    @ConsumerName("CLIENT-OUT")
    @GroupId("asd-out")
    public void readClientOut(Client client) throws Exception {
      increment(readClientOutRuns, new SimpleDateFormat("HH:mm:ss").format(new Date()));
      insertClient("CLIENT-OUT", client, "client_id");
      if (sleepClientOut.get() > 0) {
        Thread.sleep(sleepClientOut.get());
      }
    }

    @Topic("CLIENT-OUT")
    @ConsumerName("CLIENT-OUT-2")
    @GroupId("asd-out-2")
    public void readClientOut2(Client client) throws Exception {
      increment(readClientOut2Runs, new SimpleDateFormat("HH:mm:ss").format(new Date()));
      insertClient("CLIENT-OUT-2", client, "client_id2");
      if (sleepClientOut2.get() > 0) {
        Thread.sleep(sleepClientOut2.get());
      }
    }

    @SuppressWarnings("SameParameterValue")
    private void insertClient(String consumerName, Client client, String table) throws SQLException {

      try (Connection connection = dataSource.getConnection()) {

        //noinspection SqlResolve
        String sql = "insert into " + table + " (id, consumer_name) values (?, ?)";

        try (PreparedStatement ps = connection.prepareStatement(sql)
        ) {
          ps.setString(1, client.id);
          ps.setString(2, consumerName);
          ps.executeUpdate();
        }

      }

      if (printClientToStdout.get()) {
        System.out.println("Come client " + client + " from " + Thread.currentThread().getName());
      }

    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    Path pwd = new File(".").getAbsoluteFile().toPath().normalize();

    Path workingDir = pwd.resolve("build").resolve("MassiveTestServer");

    Files.createDirectories(workingDir);

    Path workingFile = workingDir.resolve("working.file");

    workingFile.toFile().createNewFile();

    String kafkaServers = "localhost:9091,localhost:9092,localhost:9093,localhost:9094";
    String zookeeperServers = "localhost:2181,localhost:2182,localhost:2183";

    Map<String, Object> conf = new HashMap<>();
    conf.put("bootstrap.servers", kafkaServers);

    try (AdminClient adminClient = KafkaAdminClient.create(conf)) {

      Path existsDir = workingDir.resolve("exists");
      existsDir.toFile().mkdirs();

      Path clientExistsFile = existsDir.resolve("CLIENT");
      if (!clientExistsFile.toFile().exists()) {
        NewTopic newTopic = new NewTopic("CLIENT", 480, (short) 2);
        adminClient.createTopics(singletonList(newTopic)).all();
        clientExistsFile.toFile().createNewFile();
      }

      Path clientOutExistsFile = existsDir.resolve("CLIENT-OUT");
      if (!clientOutExistsFile.toFile().exists()) {
        NewTopic newTopic = new NewTopic("CLIENT-OUT", 480, (short) 2);
        adminClient.createTopics(singletonList(newTopic)).all();
        clientOutExistsFile.toFile().createNewFile();
      }

    }

    Consumers consumers = new Consumers();

    KafkaReactorImpl reactor = createReactor(kafkaServers, zookeeperServers);

    reactor.addController(consumers);

    System.out.println("Before start consumers");

    reactor.startConsumers();
    try {

      System.out.println("Started consumers");

      ProducerFacade mainProducer = reactor.createProducer("main");

      System.out.println("Start waiting process");

      LongParameter portion = new LongParameter(workingDir, "portion", 300L);
      LongParameter portionCount = new LongParameter(workingDir, "portionCount", 3L);
      LongParameter sleepClientOut = new LongParameter(workingDir, "sleepClientOut", consumers.sleepClientOut);
      LongParameter sleepClientOut2 = new LongParameter(workingDir, "sleepClientOut2", consumers.sleepClientOut2);

      BoolParameter printClientToStdoutP = new BoolParameter(workingDir, "printClientToStdout", printClientToStdout);
      BoolParameter generateErrorsP = new BoolParameter(workingDir, "generateErrors", generateErrors);

      BoolParameter insertClientPortionParallel = new BoolParameter(workingDir, "insertClientPortionParallel", true);
      Command insertClientPortion = new Command(workingDir, "insertClientPortion");

      ClientPortionInserting clientPortionInserting = new ClientPortionInserting(
        portion, portionCount, mainProducer, insertClientPortionParallel, workingFile, insertClientPortion
      );

      Command reportsShow = new Command(workingDir, "reportsShow");
      Command reportsClear = new Command(workingDir, "reportsClear");

      while (workingFile.toFile().exists()) {

        portion.ping();
        portionCount.ping();
        sleepClientOut.ping();
        sleepClientOut2.ping();
        printClientToStdoutP.ping();
        generateErrorsP.ping();
        insertClientPortionParallel.ping();

        clientPortionInserting.ping();

        if (reportsShow.run()) {
          printReports(workingDir);
        }
        if (reportsClear.run()) {
          readClientRuns.clear();
          readClientOutRuns.clear();
          System.out.println("Reports cleared");
        }

        Thread.sleep(700);

      }

    } finally {
      System.out.println("Stopping reactor");
      reactor.stopConsumers();
    }

    printReports(workingDir);

    System.out.println("Finished");


  }

  private static void printReports(Path workingDir) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("HH-mm-ss");
    String suffix = sdf.format(new Date());
    {
      Path reportsFile = workingDir.resolve("reports").resolve(suffix + "-a-readClientRuns.txt");
      printReportTo(readClientRuns, reportsFile);
    }
    {
      Path reportsFile = workingDir.resolve("reports").resolve(suffix + "-b-readClientOutRuns.txt");
      printReportTo(readClientOutRuns, reportsFile);
    }

    System.out.println("Reports printed");
  }

  private static void printReportTo(ConcurrentHashMap<String, AtomicLong> countMap, Path reportsFile) throws IOException {
    List<String> lines = countMap
      .entrySet()
      .stream()
      .sorted(Comparator.comparing(Map.Entry::getKey))
      .map(e -> e.getKey() + " " + e.getValue().get())
      .collect(Collectors.toList());

    reportsFile.toFile().getParentFile().mkdirs();

    Files.write(reportsFile, lines);

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

    ccd.addDefinition(" Int out.worker.count         0  ");
    ccd.addDefinition(" Int out.poll.duration.ms  2000  ");

    reactor.consumerConfigDefaults = ccd;

    reactor.logger().setDestination(new SimplePrinter());
    reactor.logger().setShowLogger(LoggerType.SHOW_CONSUMER_WORKER_CONFIG, false);
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

  private static void increment(ConcurrentHashMap<String, AtomicLong> countMap, String key) {
    countMap.computeIfAbsent(key, x -> new AtomicLong(0)).incrementAndGet();
  }
}
