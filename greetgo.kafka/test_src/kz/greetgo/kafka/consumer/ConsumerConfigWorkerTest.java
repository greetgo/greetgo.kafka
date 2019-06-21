package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.config.EventConfigStorageInMem;
import kz.greetgo.kafka.util.StrUtil;
import kz.greetgo.kafka.util.TestHandler;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static kz.greetgo.kafka.util.StrUtil.findFirstContains;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.data.MapEntry.entry;

public class ConsumerConfigWorkerTest {

  @Test
  public void testInitialStateInConfigStorage() {

    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    assertThat(configStorage.exists("root/controller/method.conf")).isTrue();
    assertThat(configStorage.exists("root/controller/method.d/host-id.conf")).isTrue();
    assertThat(configStorage.exists("root/controller/method.d/host-id.actual-values")).isTrue();

    List<String> parentLines = configStorage.getLinesWithoutSpaces("root/controller/method.conf");
    {
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

      assertThat(parentLines).contains("out.worker.count=1");
      assertThat(parentLines).contains("out.poll.duration.ms=800");
    }

    List<String> itKeyValues = configStorage.getLinesWithoutSpaces("root/controller/method.d/host-id.conf");
    {
      assertThat(itKeyValues).isNotNull();
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

      assertThat(itKeyValues).contains("out.worker.count:inherits");
      assertThat(itKeyValues).contains("out.poll.duration.ms:inherits");
    }

    consumerConfigWorker.close();
  }

  @Test
  public void getConfigMap() {
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms=44444",
      "con.max.poll.interval.ms=77777",
      "con.example.variable = navigator of life",
      "con.another.var = parent status quo"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "extends=root/parent.txt",
      "con.fetch.max.wait.ms=111",
      "con.send.buffer.bytes=222",
      "out.worker.count=17",
      "con.example.variable : inherits",
      "con.another.var = status quo"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    //
    //
    Map<String, Object> configMap = consumerConfigWorker.getConfigMap();
    //
    //

    assertThat(configMap).isNotNull();
    assert configMap != null;

    assertThat(configMap).contains(entry("example.variable", "navigator of life"));
    assertThat(configMap).contains(entry("another.var", "status quo"));

    consumerConfigWorker.close();
  }

  @Test
  public void getWorkerCount_direct() {
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms=44444",
      "con.max.poll.interval.ms=77777"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.fetch.max.wait.ms=111",
      "con.send.buffer.bytes=222",
      "out.worker.count = 173"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

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
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms=44444",
      "con.max.poll.interval.ms=77777",
      "out.worker.count = 728"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.fetch.max.wait.ms=111",
      "con.send.buffer.bytes=222",
      "out.worker.count : inherits"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

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
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms=44444",
      "con.max.poll.interval.ms=77777",
      "out.worker.count = 728"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.fetch.max.wait.ms=111",
      "con.send.buffer.bytes=222"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    //
    //
    int workerCount = consumerConfigWorker.getWorkerCount();
    //
    //

    assertThat(workerCount).isEqualTo(0);

    consumerConfigWorker.close();
  }

  @Test
  public void getWorkerCount_errorValue_direct() {
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms=44444",
      "con.max.poll.interval.ms=77777",
      "out.worker.count = 728"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.fetch.max.wait.ms=111",
      "con.send.buffer.bytes=222",
      "out.worker.count = left value"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    //
    //
    int workerCount = consumerConfigWorker.getWorkerCount();
    //
    //

    assertThat(workerCount).isEqualTo(0);

    String errorsPath = "root/controller/method.d/host-id.errors";

    assertThat(configStorage.exists(errorsPath)).isTrue();

    List<String> list = configStorage.readLines(errorsPath);

    assertThat(list).isNotEmpty();

    String error = findFirstContains(list, "out.worker.count");

    assertThat(error).isNotNull();
    assert error != null;
    assertThat(error.toLowerCase()).contains("parameter `out.worker.count` is absent");

    consumerConfigWorker.close();
  }

  @Test
  public void getWorkerCount_errorValue_parent() {
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms=44444",
      "con.max.poll.interval.ms=77777",
      "out.worker.count = left value"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.fetch.max.wait.ms=111",
      "con.send.buffer.bytes=222",
      "out.worker.count : inherits"
    );

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    //
    //
    int workerCount = consumerConfigWorker.getWorkerCount();
    //
    //

    assertThat(workerCount).isEqualTo(0);

    String errorsPath = "root/controller/method.d/host-id.errors";

    assertThat(configStorage.exists(errorsPath)).isTrue();

    String parentErrorsPath = "root/controller/method.errors";

    assertThat(configStorage.exists(parentErrorsPath)).isTrue();

    List<String> list = configStorage.readLines(parentErrorsPath);

    assertThat(list).isNotEmpty();

    String error = findFirstContains(list, "out.worker.count");

    assertThat(error).isNotNull();
    assert error != null;
    assertThat(error.toLowerCase()).contains("parameter `out.worker.count` is absent");

    consumerConfigWorker.close();
  }

  /**
   * <p>
   * Надо проверить, что если файл изменился, то система автоматически обновилась
   * </p>
   * <p>
   * Проверяем только родительский файл
   * </p>
   */
  @Test
  public void changedConfigStateAfterStart_fromParents() {

    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();
    testHandler.happenCount = 0;

    //Начальное состояние файлов

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms = 44444",
      "con.max.poll.interval.ms = 77777",
      "out.worker.count = 37"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.fetch.max.wait.ms = 111",
      "con.send.buffer.bytes = 222",
      "con.max.poll.interval.ms : inherits",
      "out.worker.count : inherits"
    );

    // Запускается система

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

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

    configStorage.removeLines("root/controller/method.conf",
      "con.max.poll.interval.ms = 77777",
      "out.worker.count = 37"
    );
    configStorage.addLines("root/controller/method.conf",
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

  /**
   * <p>
   * Надо проверить, что если файл изменился, то система автоматически обновилась
   * </p>
   * <p>
   * Проверяем только файл хоста
   * </p>
   */
  @Test
  public void changedConfigStateAfterStart_direct() {

    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();
    testHandler.happenCount = 0;

    //Начальное состояние файлов-конфигов

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms = 44444",
      "con.max.poll.interval.ms = 77777"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.send.buffer.bytes = 222",
      "out.worker.count = 37"
    );

    // Запускается система

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

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

    configStorage.removeLines("root/controller/method.d/host-id.conf",
      "con.send.buffer.bytes = 222",
      "out.worker.count = 37"
    );
    configStorage.addLines("root/controller/method.d/host-id.conf",
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

  /**
   * Нужно проверить, что если вначале файл был без ошибок, а потом ошибки появились - должен создаться файл ошибок
   */
  @Test
  public void checkCreatesErrorFileAfterBadUpdate_parent() {
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    // Вначале в файле-конфиге ошибок нет

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms = 44444",
      "out.worker.count = 37"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.session.timeout.ms : inherits",
      "out.worker.count : inherits"
    );

    // Стартуем приложение

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    // Ошибок нет - смотрим, что файла тоже нет

    assertThat(configStorage.exists("root/controller/method.errors")).isFalse();

    // Меняем файл, делая в нём ошибку

    configStorage.rememberState();

    configStorage.removeLines("root/controller/method.conf",
      "out.worker.count = 37"
    );
    configStorage.addLines("root/controller/method.conf",
      "out.worker.count = err"
    );

    configStorage.fireEvents();

    // Надо что-то прочитать

    consumerConfigWorker.getWorkerCount();

    // Смотрим, что появился файл ошибок

    assertThat(configStorage.exists("root/controller/method.errors")).isTrue();

    List<String> errorLines = configStorage.readLines("root/controller/method.errors");
    System.out.println(errorLines);

    // содержимое файла-ошибок здесь проверять не надо, так как это должно быть в отдельных тестах

    // Ну и проверим, что хэндлер отработал

    assertThat(testHandler.happenCount).isEqualTo(1);

    consumerConfigWorker.close();
  }

  /**
   * Нужно проверить, что если вначале файл был без ошибок, а потом ошибки появились - должен создаться файл ошибок
   */
  @Test
  public void checkCreatesErrorFileAfterBadUpdate_direct() {

    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    // В начале в файлах-конфигах ошибок нет

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms = 44444",
      "out.worker.count = 37"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.session.timeout.ms : inherits",
      "out.worker.count : inherits"
    );

    // Стартуем приложение

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    // Ошибок нет - смотрим, что файла ошибок тоже нет

    String hostErrorFile = "root/controller/method.d/host-id.errors";

    assertThat(configStorage.exists(hostErrorFile)).isFalse();

    // Меняем файл, делая в нём ошибку

    configStorage.rememberState();

    configStorage.removeLines("root/controller/method.d/host-id.conf",
      "out.worker.count : inherits"
    );
    configStorage.addLines("root/controller/method.d/host-id.conf",
      "out.worker.count : err"
    );

    configStorage.fireEvents();

    // Надо что-то прочитать

    consumerConfigWorker.getWorkerCount();

    // Смотрим, что появился файл ошибок

    assertThat(configStorage.exists(hostErrorFile)).isTrue();

    List<String> errorLines = configStorage.readLines(hostErrorFile);
    System.out.println(errorLines);

    // содержимое файла-ошибок здесь проверять не надо, так как это должно быть в отдельных тестах

    // Ну и проверим, что хэндлер отработал

    assertThat(testHandler.happenCount).isEqualTo(1);

    consumerConfigWorker.close();
  }

  /**
   * Нужно проверить, что файл ошибок удаляется, если ошибки исправлены
   */
  @Test
  public void errorFilesDeletesAfterErrorDeleted_parents() {
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    // В начале в файлах-конфигах делаем ошибки

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms = 44444",
      "out.worker.count = err"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.session.timeout.ms : inherits",
      "out.worker.count : inherits"
    );

    // Стартуем приложение

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    consumerConfigWorker.getWorkerCount();

    // Должен быть файл ошибок - проверим это

    String parentErrorFile = "root/controller/method.errors";

    assertThat(configStorage.exists(parentErrorFile)).isTrue();

    // Меняем файл, исправляя ошибки

    configStorage.rememberState();

    configStorage.removeLines("root/controller/method.conf",
      "out.worker.count = err"
    );
    configStorage.addLines("root/controller/method.conf",
      "out.worker.count = 117"
    );

    configStorage.fireEvents();

    // Надо что-то прочитать

    consumerConfigWorker.getWorkerCount();

    // Смотрим, что файл ошибок исчез

    if (configStorage.exists(parentErrorFile)) {
      List<String> errorLines = configStorage.readLines(parentErrorFile);
      System.out.println(errorLines);
    }

    assertThat(configStorage.exists(parentErrorFile)).isFalse();

    // Ну и проверим, что хэндлер отработал

    assertThat(testHandler.happenCount).isEqualTo(1);

    consumerConfigWorker.close();
  }

  private void printFile(String fileName, List<String> lines) {
    System.out.println("BEGIN file " + fileName);
    if (lines.size() > 0) {
      int len = ("" + (lines.size() - 1)).length();
      int i = 1;
      for (String line : lines) {
        System.out.println(StrUtil.intToStrLen(i, len) + " " + line);
        i++;
      }
    }
    System.out.println("END file " + fileName);
  }

  /**
   * Нужно проверить, что файл ошибок удаляется, если ошибки исправлены
   */
  @Test
  public void errorFilesDeletesAfterErrorDeleted_direct() {
    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();
    TestHandler testHandler = new TestHandler();

    // В начале в файлах-конфигах делаем ошибки

    configStorage.addLines("root/controller/method.conf",
      "con.session.timeout.ms = 44444",
      "out.worker.count = 331"
    );

    configStorage.addLines("root/controller/method.d/host-id.conf",
      "con.session.timeout.ms : inherits",
      "out.worker.count : err"
    );

    // Стартуем приложение

    ConsumerConfigWorker consumerConfigWorker = new ConsumerConfigWorker(() -> configStorage, testHandler);

    consumerConfigWorker.setConfigPathPrefix("root/controller/method");
    consumerConfigWorker.setHostId("host-id");

    consumerConfigWorker.start();

    // Должен быть файл ошибок - проверим это

    String errorFile = "root/controller/method.d/host-id.errors";

    assertThat(configStorage.exists(errorFile)).isTrue();

    // Меняем файл, исправляя ошибки

    configStorage.rememberState();

    configStorage.removeLines("root/controller/method.d/host-id.conf",
      "out.worker.count : err"
    );
    configStorage.addLines("root/controller/method.d/host-id.conf",
      "out.worker.count = 223"
    );

    configStorage.fireEvents();

    // Надо что-то прочитать

    consumerConfigWorker.getWorkerCount();

    // Смотрим, что файл ошибок исчез

    if (configStorage.exists(errorFile)) {
      List<String> errorLines = configStorage.readLines(errorFile);
      printFile(errorFile, errorLines);
    }

    assertThat(configStorage.exists(errorFile)).isFalse();

    // Ну и проверим, что хэндлер отработал

    assertThat(testHandler.happenCount).isEqualTo(1);

    consumerConfigWorker.close();
  }

}
