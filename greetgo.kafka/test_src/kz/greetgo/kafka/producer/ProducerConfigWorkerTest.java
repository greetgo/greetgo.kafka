package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.config.ConfigStorageInMem;
import org.fest.assertions.data.MapEntry;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class ProducerConfigWorkerTest {

  @Test
  public void getConfigFor_defaultValues() {

    ConfigStorageInMem configStorage = new ConfigStorageInMem();

    ProducerConfigWorker producerConfigWorker = new ProducerConfigWorker(() -> "producer/root", () -> configStorage);

    //
    //
    Map<String, Object> config = producerConfigWorker.getConfigFor("all-test");
    //
    //

    assertThat(config).contains(MapEntry.entry("acts", "all"));
    assertThat(config).contains(MapEntry.entry("buffer.memory", "33554432"));
    assertThat(config).contains(MapEntry.entry("retries", "2147483647"));
    assertThat(config).contains(MapEntry.entry("compression.type", "none"));
    assertThat(config).contains(MapEntry.entry("batch.size", "16384"));
    assertThat(config).contains(MapEntry.entry("connections.max.idle.ms", "540000"));
    assertThat(config).contains(MapEntry.entry("delivery.timeout.ms", "35000"));
    assertThat(config).contains(MapEntry.entry("request.timeout.ms", "30000"));
    assertThat(config).contains(MapEntry.entry("linger.ms", "1"));
    assertThat(config).contains(MapEntry.entry("batch.size", "16384"));

    List<String> lines = configStorage.getLinesWithoutSpaces("producer/root/all-test.txt");

    assertThat(lines).contains("prod.acts=all");
    assertThat(lines).contains("prod.buffer.memory=33554432");
    assertThat(lines).contains("prod.retries=2147483647");
    assertThat(lines).contains("prod.compression.type=none");
    assertThat(lines).contains("prod.batch.size=16384");
    assertThat(lines).contains("prod.connections.max.idle.ms=540000");
    assertThat(lines).contains("prod.delivery.timeout.ms=35000");
    assertThat(lines).contains("prod.request.timeout.ms=30000");
    assertThat(lines).contains("prod.linger.ms=1");
    assertThat(lines).contains("prod.batch.size=16384");
  }

  @Test
  public void readingFromConfigFile() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();

    configStorage.addLines("producer/root/testProducer.txt",
        "prod.param1=value 1001",
        "prod.param2=value 1002",
        "prod.param3=value 1003",
        "prod.param4=value 1004"
    );

    ProducerConfigWorker producerConfigWorker = new ProducerConfigWorker(() -> "producer/root", () -> configStorage);

    //
    //
    Map<String, Object> config = producerConfigWorker.getConfigFor("testProducer");
    //
    //

    assertThat(config).contains(MapEntry.entry("param1", "value 1001"));
    assertThat(config).contains(MapEntry.entry("param2", "value 1002"));
    assertThat(config).contains(MapEntry.entry("param3", "value 1003"));
    assertThat(config).contains(MapEntry.entry("param4", "value 1004"));
  }

  @Test
  public void updatesDataAfterConfigChanged() {

    ConfigStorageInMem configStorage = new ConfigStorageInMem();

    configStorage.addLines("producer/root/testProducer.txt",
        "prod.param1=started value"
    );

    ProducerConfigWorker producerConfigWorker = new ProducerConfigWorker(() -> "producer/root", () -> configStorage);

    Map<String, Object> startedConfig = producerConfigWorker.getConfigFor("testProducer");

    assertThat(startedConfig).contains(MapEntry.entry("param1", "started value"));

    configStorage.rememberState();

    configStorage.removeLines("producer/root/testProducer.txt",
        "prod.param1=started value"
    );
    configStorage.addLines("producer/root/testProducer.txt",
        "prod.param1=another value"
    );

    configStorage.fireEvents();

    //
    //
    Map<String, Object> config = producerConfigWorker.getConfigFor("testProducer");
    //
    //

    assertThat(config).contains(MapEntry.entry("param1", "another value"));

    //
    //
    producerConfigWorker.close();
    //
    //

    configStorage.rememberState();


    configStorage.removeLines("producer/root/testProducer.txt",
        "prod.param1=another value"
    );
    configStorage.removeLines("producer/root/testProducer.txt",
        "prod.param1=last value"
    );

    configStorage.fireEvents();

    //
    //
    Map<String, Object> last = producerConfigWorker.getConfigFor("testProducer");
    //
    //

    assertThat(last).contains(MapEntry.entry("param1", "another value"));
  }
}
