package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.core.config.ConfigStorageInMem;
import kz.greetgo.kafka2.util.Handler;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static kz.greetgo.kafka2.util.StrUtil.bytesToLines;
import static kz.greetgo.kafka2.util.StrUtil.findFirstContains;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.data.MapEntry.entry;

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

    consumerConfigWorker.close();
  }

  @Test
  public void automaticallyAddAbsentConfigParameters() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms=44444",
        "con.max.poll.interval.ms=77777"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.fetch.max.wait.ms=111",
        "con.send.buffer.bytes=222",
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
    assertThat(itKeyValues).contains("con.send.buffer.bytes=222");
    assertThat(itKeyValues).contains("con.fetch.max.wait.ms=111");

    assertThat(itKeyValues).contains("out.worker.count=17");
    assertThat(itKeyValues).contains("out.poll.duration.ms=800");

    consumerConfigWorker.close();
  }

  @Test
  public void getConfigMap() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms=44444",
        "con.max.poll.interval.ms=77777",
        "con.example.variable = navigator of life"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.fetch.max.wait.ms=111",
        "con.send.buffer.bytes=222",
        "out.worker.count=17",
        "con.example.variable : inherits"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    //
    //
    Map<String, Object> configMap = consumerConfigWorker.getConfigMap();
    //
    //

    assertThat(configMap).isNotNull();
    assert configMap != null;

    assertThat(configMap).contains(entry("example.variable", "navigator of life"));

    consumerConfigWorker.close();
  }

  @Test
  public void getWorkerCount_direct() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms=44444",
        "con.max.poll.interval.ms=77777"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.fetch.max.wait.ms=111",
        "con.send.buffer.bytes=222",
        "out.worker.count = 173"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    //
    //
    int workerCount = consumerConfigWorker.getWorkerCount();
    //
    //

    assertThat(workerCount).isEqualTo(173);

    consumerConfigWorker.close();
  }

  @Test
  public void getWorkerCount_inherits() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms=44444",
        "con.max.poll.interval.ms=77777",
        "out.worker.count = 728"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.fetch.max.wait.ms=111",
        "con.send.buffer.bytes=222",
        "out.worker.count : inherits"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    //
    //
    int workerCount = consumerConfigWorker.getWorkerCount();
    //
    //

    assertThat(workerCount).isEqualTo(728);

    consumerConfigWorker.close();
  }


  @Test
  public void getWorkerCount_defaultValue() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms=44444",
        "con.max.poll.interval.ms=77777",
        "out.worker.count = 728"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.fetch.max.wait.ms=111",
        "con.send.buffer.bytes=222"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    //
    //
    int workerCount = consumerConfigWorker.getWorkerCount();
    //
    //

    assertThat(workerCount).isEqualTo(1);

    consumerConfigWorker.close();
  }

  @Test
  public void getWorkerCount_errorValue_direct() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms=44444",
        "con.max.poll.interval.ms=77777",
        "out.worker.count = 728"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.fetch.max.wait.ms=111",
        "con.send.buffer.bytes=222",
        "out.worker.count = left value"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    //
    //
    int workerCount = consumerConfigWorker.getWorkerCount();
    //
    //

    assertThat(workerCount).isEqualTo(0);

    String errorsPath = "root/controller/method.txt.errors.txt";

    assertThat(configStorage.exists(errorsPath)).isTrue();

    List<String> list = bytesToLines(configStorage.readContent(errorsPath));

    assertThat(list).isNotEmpty();

    String error = findFirstContains(list, "out.worker.count");

    assertThat(error).isNotNull();
    assert error != null;
    assertThat(error.toLowerCase()).contains("line 4");

    consumerConfigWorker.close();
  }

  @Test
  public void getWorkerCount_errorValue_parent() {
    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms=44444",
        "con.max.poll.interval.ms=77777",
        "out.worker.count = left value"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.fetch.max.wait.ms=111",
        "con.send.buffer.bytes=222",
        "out.worker.count : inherits"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    //
    //
    int workerCount = consumerConfigWorker.getWorkerCount();
    //
    //

    assertThat(workerCount).isEqualTo(0);

    String errorsPath = "root/controller/method.txt.errors.txt";

    assertThat(configStorage.exists(errorsPath)).isFalse();

    String parentErrorsPath = "root/parent.txt.errors.txt";

    assertThat(configStorage.exists(parentErrorsPath)).isTrue();

    List<String> list = bytesToLines(configStorage.readContent(parentErrorsPath));

    assertThat(list).isNotEmpty();

    String error = findFirstContains(list, "out.worker.count");

    assertThat(error).isNotNull();
    assert error != null;
    assertThat(error.toLowerCase()).contains("line 3");

    consumerConfigWorker.close();
  }

  @Test
  public void changedConfigStateAfterStart_fromParents() {
    //Надо проверить, что если файл изменился, то система автоматически обновилась

    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();
    testHandler.happenCount = 0;

    //Начальное состояние файлов

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms = 44444",
        "con.max.poll.interval.ms = 77777",
        "out.worker.count = 37"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.fetch.max.wait.ms = 111",
        "con.send.buffer.bytes = 222",
        "out.worker.count : inherits"
    );

    // Запускается система

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    // Проверяем, что всё прочиталось

    {
      int workerCount = consumerConfigWorker.getWorkerCount();
      assertThat(workerCount).isEqualTo(37);

      String maxPollIntervalMs = (String) consumerConfigWorker.getConfigMap().get("max.poll.interval.ms");
      assertThat(maxPollIntervalMs).isEqualTo("77777");
    }

    // Теперь меняем конфиги

    configStorage.rememberState();

    configStorage.removeLines("root/parent.txt",
        "con.max.poll.interval.ms = 77777",
        "out.worker.count = 37"
    );
    configStorage.addLines("root/parent.txt",
        "con.max.poll.interval.ms = 454545",
        "out.worker.count = 987"
    );

    configStorage.fireEvents();

    // И смотрим, что система обновила значения на новые из конфигов

    {
      int workerCount = consumerConfigWorker.getWorkerCount();
      assertThat(workerCount).isEqualTo(987);

      String maxPollIntervalMs = (String) consumerConfigWorker.getConfigMap().get("max.poll.interval.ms");
      assertThat(maxPollIntervalMs).isEqualTo("454545");
    }

    // Ну и проверим, что хэндлер отработал

    assertThat(testHandler.happenCount).isEqualTo(1);

    consumerConfigWorker.close();
  }

  @Test
  public void changedConfigStateAfterStart_direct() {
    //Надо проверить, что если файл изменился, то система автоматически обновилась

    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();
    testHandler.happenCount = 0;

    //Начальное состояние файлов

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms = 44444",
        "con.max.poll.interval.ms = 77777"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.send.buffer.bytes = 222",
        "out.worker.count = 37"
    );

    // Запускается система

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    // Проверяем, что всё прочиталось

    {
      int workerCount = consumerConfigWorker.getWorkerCount();
      assertThat(workerCount).isEqualTo(37);

      String sendBufferBytes = (String) consumerConfigWorker.getConfigMap().get("send.buffer.bytes");
      assertThat(sendBufferBytes).isEqualTo("222");
    }

    // Теперь меняем конфиги

    configStorage.rememberState();

    configStorage.removeLines("root/controller/method.txt",
        "con.send.buffer.bytes = 222",
        "out.worker.count = 37"
    );
    configStorage.addLines("root/controller/method.txt",
        "con.send.buffer.bytes = 34565",
        "out.worker.count = 68451"
    );

    configStorage.fireEvents();

    // И смотрим, что система обновила значения на новые из конфигов

    {
      int workerCount = consumerConfigWorker.getWorkerCount();
      assertThat(workerCount).isEqualTo(68451);

      String sendBufferBytes = (String) consumerConfigWorker.getConfigMap().get("send.buffer.bytes");
      assertThat(sendBufferBytes).isEqualTo("34565");
    }

    // Ну и проверим, что хэндлер отработал

    assertThat(testHandler.happenCount).isEqualTo(1);

    consumerConfigWorker.close();
  }

  @Test
  public void checkCreatesErrorFileAfterBadUpdate_parent() {
    // Нужно проверить, что если вначале файл был без ошибок, а потом ошибки появились - должен создаться файл ошибок

    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    // Начальное состояние файлов - ошибок нет

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms = 44444",
        "out.worker.count = 37"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.session.timeout.ms : inherits",
        "out.worker.count : inherits"
    );

    // Стартуем приложение

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    // Ошибок нет - смотрим, что файла тоже нет

    assertThat(configStorage.exists("root/parent.txt.errors.txt")).isFalse();

    // Меняем файл, делая в нём ошибку

    configStorage.rememberState();

    configStorage.removeLines("root/parent.txt",
        "out.worker.count = 37"
    );
    configStorage.addLines("root/parent.txt",
        "out.worker.count = err"
    );

    configStorage.fireEvents();

    // Надо что-то прочитать

    consumerConfigWorker.getWorkerCount();

    // Смотрим, что появился файл ошибок

    assertThat(configStorage.exists("root/parent.txt.errors.txt")).isTrue();

    String errorsText = new String(configStorage.readContent("root/parent.txt.errors.txt"), UTF_8);
    System.out.println(errorsText);
  }

  @Test
  public void checkCreatesErrorFileAfterBadUpdate_direct() {
    // Нужно проверить, что если вначале файл был без ошибок, а потом ошибки появились - должен создаться файл ошибок

    ConfigStorageInMem configStorage = new ConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    // Начальное состояние файлов - ошибок нет

    configStorage.addLines("root/parent.txt",
        "con.session.timeout.ms = 44444",
        "out.worker.count = 37"
    );

    configStorage.addLines("root/controller/method.txt",
        "extends=root/parent.txt",
        "con.session.timeout.ms : inherits",
        "out.worker.count : inherits"
    );

    // Стартуем приложение

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setParentPath("root/parent.txt");
    consumerConfigWorker.setConfigPath("root/controller/method.txt");

    consumerConfigWorker.start();

    // Ошибок нет - смотрим, что файла тоже нет

    assertThat(configStorage.exists("root/controller/method.txt.errors.txt")).isFalse();

    // Меняем файл, делая в нём ошибку

    configStorage.rememberState();

    configStorage.removeLines("root/controller/method.txt",
        "out.worker.count : inherits"
    );
    configStorage.addLines("root/controller/method.txt",
        "out.worker.count : err"
    );

    configStorage.fireEvents();

    // Надо что-то прочитать

    consumerConfigWorker.getWorkerCount();

    // Смотрим, что появился файл ошибок

    assertThat(configStorage.exists("root/controller/method.txt.errors.txt")).isTrue();

    String errorsText = new String(configStorage.readContent("root/controller/method.txt.errors.txt"), UTF_8);
    System.out.println(errorsText);
  }
}
