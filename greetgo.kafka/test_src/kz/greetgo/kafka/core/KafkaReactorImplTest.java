package kz.greetgo.kafka.core;

import kz.greetgo.kafka.ModelKryo;
import kz.greetgo.kafka.ModelKryo2;
import kz.greetgo.kafka.consumer.TestLoggerDestinationInteractive;
import kz.greetgo.kafka.consumer.annotations.ConsumersFolder;
import kz.greetgo.kafka.consumer.annotations.GroupId;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.core.config.EventConfigStorageZooKeeper;
import kz.greetgo.kafka.core.logger.LoggerDestination;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.kafka.util.NetUtil;
import kz.greetgo.strconverter.simple.StrConverterSimple;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import static kz.greetgo.kafka.core.config.EventConfigStorageUtil.changeRoot;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class KafkaReactorImplTest {

  String bootstrapServers;

  @BeforeMethod
  public void pingKafka() {
    bootstrapServers = "localhost:9092";

    if (!NetUtil.canConnectToAnyBootstrapServer(bootstrapServers)) {
      throw new SkipException("No kafka connection : " + bootstrapServers);
    }
  }

  @ConsumersFolder("top")
  public static class TestController {

    @Topic("test_topic1")
    @GroupId("gr1-2")
    public void consumer1(ModelKryo model) {
      System.out.println("Come from consumer1: " + model);
    }

    @Topic("test_topic2")
    @GroupId("gr2")
    public void consumer2(ModelKryo2 model) {
      System.out.println("Come from consumer2: " + model);
    }

  }

  @Test
  public void runKafkaReactor() throws Exception {
    EventConfigStorageZooKeeper storageZooKeeper = new EventConfigStorageZooKeeper(
      "greetgo_kafka/KafkaReactorImplTest", () -> "localhost:2181", () -> 3000
    );

    TestController controller = new TestController();

    LoggerDestination testConsumerLogger = new TestLoggerDestinationInteractive();

    KafkaReactor kafkaReactor = new KafkaReactorImpl();
    kafkaReactor.setConsumerConfigStorage(changeRoot("consumers", storageZooKeeper));
    kafkaReactor.setProducerConfigStorage(changeRoot("producers", storageZooKeeper));

    kafkaReactor.addController(controller);

    StrConverterSimple strConverter = new StrConverterSimple();
    strConverter.convertRegistry().register(ModelKryo.class);
    strConverter.convertRegistry().register(ModelKryo2.class);
    strConverter.convertRegistry().register(Box.class);

    kafkaReactor.setStrConverterSupplier(() -> strConverter);

    kafkaReactor.logger().setDestination(testConsumerLogger);

    kafkaReactor.setHostId("testHost");
    kafkaReactor.setAuthorSupplier(() -> "author123");

    kafkaReactor.setBootstrapServers(() -> bootstrapServers);

    kafkaReactor.startConsumers();

    AtomicBoolean working = new AtomicBoolean(true);

    String baseDir = "build/KafkaReactorImplTest";

    File currentKeepRunningFile = new File(baseDir + "/currentKeepRunningFile.txt");
    File keepRunningFile = new File(baseDir + "/keepRunning.txt");
    File keepRunningFile2 = new File(baseDir + "/keepRunning__removeThisSuffix.txt");

    if (!keepRunningFile.exists()) {
      keepRunningFile2.getParentFile().mkdirs();
      keepRunningFile2.createNewFile();
    }
    currentKeepRunningFile.createNewFile();

    File test_topic1_dir = new File(baseDir + "/test_topic1");
    File test_topic2_dir = new File(baseDir + "/test_topic2");

    test_topic1_dir.mkdirs();
    test_topic2_dir.mkdirs();

    ProducerFacade producer = kafkaReactor.createProducer("main");

    ProducerThread producerThread1 = new ProducerThread(producer, working,
      ModelKryo.class, test_topic1_dir, "test_topic1");
    ProducerThread producerThread2 = new ProducerThread(producer, working,
      ModelKryo2.class, test_topic2_dir, "test_topic2");

    producerThread1.start();
    producerThread2.start();

    for (int i = 0;
         test_topic1_dir.exists() && test_topic2_dir.exists() && currentKeepRunningFile.exists();
         i++) {

      Thread.sleep(600);

      if (i > 5 && !keepRunningFile.exists()) {
        break;
      }
    }

    kafkaReactor.stopConsumers();

    working.set(false);

    producerThread1.join();
    producerThread2.join();
  }

  static class ProducerThread extends Thread {

    private final ProducerFacade producer;
    private final AtomicBoolean working;
    private final Class<?> aClass;
    private final File dir;
    private final String topic;

    public ProducerThread(ProducerFacade producer, AtomicBoolean working, Class<?> aClass, File dir, String topic) {
      this.producer = producer;
      this.working = working;
      this.aClass = aClass;
      this.dir = dir;
      this.topic = topic;
    }

    @Override
    public void run() {
      try {
        runInner();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void runInner() throws Exception {
      SimpleDateFormat sdf = new SimpleDateFormat("HH-mm-ss-SSS");
      while (working.get() && dir.exists()) {

        File[] files = dir.listFiles();
        if (files != null) {
          for (File file : files) {
            Object object = readFromFile(aClass, file);

            producer
              .sending(object)
              .toTopic(topic)
              .go()
              .awaitAndGet();

            System.out.println("Object " + object + " sent to " + topic);
          }

          for (File file : files) {
            File file2 = Paths
              .get(dir.toString() + "_sent")
              .resolve(file.getName() + "_" + sdf.format(new Date()))
              .toFile();

            file2.getParentFile().mkdirs();
            file.renameTo(file2);
          }
        }

        Thread.sleep(700);
      }
    }
  }

  private static Object readFromFile(Class<?> aClass, File file) throws Exception {
    Method readFromFile = aClass.getMethod("readFromFile", File.class);
    return readFromFile.invoke(null, file);
  }
}
