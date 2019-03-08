package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.core.config.ConfigStorageInMem;
import kz.greetgo.kafka2.util.Handler;
import org.testng.annotations.Test;

import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class ConsumerConfigWorkerTest {

  static class TestHandler implements Handler {
    int happenCount = 0;

    @Override
    public void handler() {
      happenCount++;
    }
  }

  @Test
  public void testInitialStateInConfigStorage() {

    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    assertThat(configStorage.exists("root/parent.txt")).isTrue();
    assertThat(configStorage.exists("root/controller/method.txt")).isTrue();

    List<String> parentLines = configStorage.getLinesWithoutSpaces("root/parent.txt");

    assertThat(parentLines).isNotNull();
    assertThat(parentLines).contains("con.auto.commit.interval.ms=1000");
    assertThat(parentLines).contains("con.session.timeout.ms=30000");
    assertThat(parentLines).contains("con.heartbeat.interval.ms=10000");
    assertThat(parentLines).contains("con.fetch.min.bytes=1");
    assertThat(parentLines).contains("con.max.partition.fetch.bytes=1048576");
    assertThat(parentLines).contains("con.connections.max.idle.ms=540000");
    assertThat(parentLines).contains("con.default.api.timeout.ms=60000");
    assertThat(parentLines).contains("con.fetch.max.bytes=52428800");
    assertThat(parentLines).contains("con.max.poll.interval.ms=300000");
    assertThat(parentLines).contains("con.max.poll.records=500");
    assertThat(parentLines).contains("con.receive.buffer.bytes=65536");
    assertThat(parentLines).contains("con.request.timeout.ms=30000");
    assertThat(parentLines).contains("con.send.buffer.bytes=131072");
    assertThat(parentLines).contains("con.fetch.max.wait.ms=500");

    List<String> itKeyValues = configStorage.getLinesWithoutSpaces("root/controller/method.txt");
    assertThat(itKeyValues).isNotNull();
    assertThat(itKeyValues).contains("extends=root/parent.txt");
    assertThat(itKeyValues).contains("con.auto.commit.interval.ms:inherits");
    assertThat(itKeyValues).contains("con.session.timeout.ms:inherits");
    assertThat(itKeyValues).contains("con.heartbeat.interval.ms:inherits");
    assertThat(itKeyValues).contains("con.fetch.min.bytes:inherits");
    assertThat(itKeyValues).contains("con.max.partition.fetch.bytes:inherits");
    assertThat(itKeyValues).contains("con.connections.max.idle.ms:inherits");
    assertThat(itKeyValues).contains("con.default.api.timeout.ms:inherits");
    assertThat(itKeyValues).contains("con.fetch.max.bytes:inherits");
    assertThat(itKeyValues).contains("con.max.poll.interval.ms:inherits");
    assertThat(itKeyValues).contains("con.max.poll.records:inherits");
    assertThat(itKeyValues).contains("con.receive.buffer.bytes:inherits");
    assertThat(itKeyValues).contains("con.request.timeout.ms:inherits");
    assertThat(itKeyValues).contains("con.send.buffer.bytes:inherits");
    assertThat(itKeyValues).contains("con.fetch.max.wait.ms:inherits");

    assertThat(itKeyValues).contains("out.worker.count=1");
    assertThat(itKeyValues).contains("out.poll.duration.ms=800");
  }

  @Test
  public void automaticallyAddAbsentConfigParameters() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/parent.txt",
      "session.timeout.ms=44444",
      "max.poll.interval.ms=77777"
    );

    configStorage.addLines("root/controller/method.txt",
      "fetch.max.wait.ms=111",
      "send.buffer.bytes=222",
      "out.worker.count=17"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    assertThat(configStorage.exists("root/parent.txt")).isTrue();
    assertThat(configStorage.exists("root/controller/method.txt")).isTrue();

    List<String> parentLines = configStorage.getLinesWithoutSpaces("root/parent.txt");

    assertThat(parentLines).isNotNull();
    assertThat(parentLines).contains("con.auto.commit.interval.ms=1000");
    assertThat(parentLines).contains("con.session.timeout.ms=44444");
    assertThat(parentLines).contains("con.heartbeat.interval.ms=10000");
    assertThat(parentLines).contains("con.fetch.min.bytes=1");
    assertThat(parentLines).contains("con.max.partition.fetch.bytes=1048576");
    assertThat(parentLines).contains("con.connections.max.idle.ms=540000");
    assertThat(parentLines).contains("con.default.api.timeout.ms=60000");
    assertThat(parentLines).contains("con.fetch.max.bytes=52428800");
    assertThat(parentLines).contains("con.max.poll.interval.ms=77777");
    assertThat(parentLines).contains("con.max.poll.records=500");
    assertThat(parentLines).contains("con.receive.buffer.bytes=65536");
    assertThat(parentLines).contains("#con.receive.buffer.bytes=-1");
    assertThat(parentLines).contains("con.request.timeout.ms=30000");
    assertThat(parentLines).contains("con.send.buffer.bytes=131072");
    assertThat(parentLines).contains("#con.send.buffer.bytes=-1");
    assertThat(parentLines).contains("con.fetch.max.wait.ms=500");

    List<String> itKeyValues = configStorage.getLinesWithoutSpaces("root/controller/method.txt");
    assertThat(itKeyValues).isNotNull();
    assertThat(itKeyValues).contains("extends=root/parent.txt");
    assertThat(itKeyValues).contains("con.auto.commit.interval.ms:inherits");
    assertThat(itKeyValues).contains("con.session.timeout.ms:inherits");
    assertThat(itKeyValues).contains("con.heartbeat.interval.ms:inherits");
    assertThat(itKeyValues).contains("con.fetch.min.bytes:inherits");
    assertThat(itKeyValues).contains("con.max.partition.fetch.bytes:inherits");
    assertThat(itKeyValues).contains("con.connections.max.idle.ms:inherits");
    assertThat(itKeyValues).contains("con.default.api.timeout.ms:inherits");
    assertThat(itKeyValues).contains("con.fetch.max.bytes:inherits");
    assertThat(itKeyValues).contains("con.max.poll.interval.ms:inherits");
    assertThat(itKeyValues).contains("con.max.poll.records:inherits");
    assertThat(itKeyValues).contains("con.receive.buffer.bytes:inherits");
    assertThat(itKeyValues).contains("con.request.timeout.ms:inherits");
    assertThat(itKeyValues).contains("con.send.buffer.bytes=222");
    assertThat(itKeyValues).contains("con.fetch.max.wait.ms=111");

    assertThat(itKeyValues).contains("out.worker.count=17");
    assertThat(itKeyValues).contains("out.poll.duration.ms=800");
  }
}
