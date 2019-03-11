package kz.greetgo.kafka2.producer;

import kz.greetgo.kafka2.core.config.ConfigStorageInMem;
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
}
