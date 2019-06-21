package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.config.ConfigEventType;
import kz.greetgo.kafka.util.StrUtil;
import kz.greetgo.kafka.util.TestEventConfigFile;
import kz.greetgo.kafka.util.TestHandler;
import org.fest.assertions.data.MapEntry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class ConsumerConfigFileWorkerTest {

  ConsumerConfigFileWorker fileWorker;

  TestHandler configDataChanged;
  TestEventConfigFile parentConfig;
  TestEventConfigFile parentConfigError;
  TestEventConfigFile hostConfig;
  TestEventConfigFile hostConfigError;
  TestEventConfigFile hostConfigActualValues;

  @BeforeMethod
  public void prepareFileWorker() {

    configDataChanged = new TestHandler();
    parentConfig = new TestEventConfigFile();
    parentConfigError = new TestEventConfigFile();
    hostConfig = new TestEventConfigFile();
    hostConfigError = new TestEventConfigFile();
    hostConfigActualValues = new TestEventConfigFile();

    fileWorker = new ConsumerConfigFileWorker(
      configDataChanged,

      parentConfig,
      parentConfigError,
      hostConfig,
      hostConfigError,
      hostConfigActualValues
    );

  }

  @Test
  public void createHostAndParentFilesWithDefaultValues() {

    fileWorker.start();

    assertThat(parentConfigError.exists()).isFalse();

    List<String> parentLines = parentConfig.readLinesWithoutSpaces();
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

    List<String> hostValues = hostConfig.readLinesWithoutSpaces();
    {
      assertThat(hostValues).isNotNull();
      assertThat(hostValues).contains("con.auto.commit.interval.ms:inherits");
      assertThat(hostValues).contains("con.session.timeout.ms:inherits");
      assertThat(hostValues).contains("con.heartbeat.interval.ms:inherits");
      assertThat(hostValues).contains("con.fetch.min.bytes:inherits");
      assertThat(hostValues).contains("con.max.partition.fetch.bytes:inherits");
      assertThat(hostValues).contains("con.connections.max.idle.ms:inherits");
      assertThat(hostValues).contains("con.default.api.timeout.ms:inherits");
      assertThat(hostValues).contains("con.fetch.max.bytes:inherits");
      assertThat(hostValues).contains("con.max.poll.interval.ms:inherits");
      assertThat(hostValues).contains("con.max.poll.records:inherits");
      assertThat(hostValues).contains("con.receive.buffer.bytes:inherits");
      assertThat(hostValues).contains("con.request.timeout.ms:inherits");
      assertThat(hostValues).contains("con.send.buffer.bytes:inherits");
      assertThat(hostValues).contains("con.fetch.max.wait.ms:inherits");

      assertThat(hostValues).contains("out.worker.count:inherits");
      assertThat(hostValues).contains("out.poll.duration.ms:inherits");
    }

    List<String> actualLines = hostConfigActualValues.readLinesWithoutSpaces();
    {
      assertThat(actualLines).isNotNull();
      assertThat(actualLines).contains("con.auto.commit.interval.ms=1000");
      assertThat(actualLines).contains("con.session.timeout.ms=30000");
      assertThat(actualLines).contains("con.heartbeat.interval.ms=10000");
      assertThat(actualLines).contains("con.fetch.min.bytes=1");
      assertThat(actualLines).contains("con.max.partition.fetch.bytes=1048576");
      assertThat(actualLines).contains("con.connections.max.idle.ms=540000");
      assertThat(actualLines).contains("con.default.api.timeout.ms=60000");
      assertThat(actualLines).contains("con.fetch.max.bytes=52428800");
      assertThat(actualLines).contains("con.max.poll.interval.ms=300000");
      assertThat(actualLines).contains("con.max.poll.records=500");
      assertThat(actualLines).contains("con.receive.buffer.bytes=65536");
      assertThat(actualLines).contains("con.request.timeout.ms=30000");
      assertThat(actualLines).contains("con.send.buffer.bytes=131072");
      assertThat(actualLines).contains("con.fetch.max.wait.ms=500");

      assertThat(actualLines).contains("out.worker.count=1");
      assertThat(actualLines).contains("out.poll.duration.ms=800");
    }

    assertThat(configDataChanged.happenCount).isZero();

  }

  @Test
  public void createHostFileWithDefaultValues() {

    parentConfig.addLines("con.auto.commit.interval.ms=54267");
    parentConfig.addLines("con.heartbeat.interval.ms=3242456");
    parentConfig.addLines("out.worker.count=1");

    parentConfig.writeContentCount = 0;

    //
    //
    fileWorker.start();
    //
    //

    if (parentConfigError.exists()) {
      System.out.println("Begin parentConfigError lines:");
      for (String line : parentConfigError.readLines()) {
        System.out.println("   " + line);
      }
      System.out.println("End parentConfigError");
    }

    assertThat(parentConfigError.exists()).isFalse();

    List<String> parentLines = hostConfig.readLinesWithoutSpaces();
    {
      assertThat(parentLines).isNotNull();
      assertThat(parentLines).contains("con.auto.commit.interval.ms:inherits");
      assertThat(parentLines).contains("con.session.timeout.ms=30000");
      assertThat(parentLines).contains("con.heartbeat.interval.ms:inherits");
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

      assertThat(parentLines).contains("out.worker.count:inherits");
      assertThat(parentLines).contains("out.poll.duration.ms=800");
    }

    List<String> actualLines = hostConfigActualValues.readLinesWithoutSpaces();
    {
      assertThat(actualLines).isNotNull();
      assertThat(actualLines).contains("con.auto.commit.interval.ms=54267");
      assertThat(actualLines).contains("con.session.timeout.ms=30000");
      assertThat(actualLines).contains("con.heartbeat.interval.ms=3242456");
      assertThat(actualLines).contains("con.fetch.min.bytes=1");
      assertThat(actualLines).contains("con.max.partition.fetch.bytes=1048576");
      assertThat(actualLines).contains("con.connections.max.idle.ms=540000");
      assertThat(actualLines).contains("con.default.api.timeout.ms=60000");
      assertThat(actualLines).contains("con.fetch.max.bytes=52428800");
      assertThat(actualLines).contains("con.max.poll.interval.ms=300000");
      assertThat(actualLines).contains("con.max.poll.records=500");
      assertThat(actualLines).contains("con.receive.buffer.bytes=65536");
      assertThat(actualLines).contains("con.request.timeout.ms=30000");
      assertThat(actualLines).contains("con.send.buffer.bytes=131072");
      assertThat(actualLines).contains("con.fetch.max.wait.ms=500");

      assertThat(actualLines).contains("out.worker.count=1");
      assertThat(actualLines).contains("out.poll.duration.ms=800");
    }

    assertThat(configDataChanged.happenCount).isZero();
    assertThat(parentConfig.writeContentCount)
      .describedAs("ATTENTION: Parent config cannot be touched because it is already exist")
      .isZero();
    assertThat(hostConfig.writeContentCount).isEqualTo(1);
    assertThat(hostConfigActualValues.writeContentCount).isEqualTo(1);

  }

  @Test
  public void createParentFileWithDefaultValues() {

    hostConfig.addLines("con.auto.commit.interval.ms=54267");
    hostConfig.addLines("con.heartbeat.interval.ms=3242456");

    hostConfig.writeContentCount = 0;

    assertThat(parentConfig.exists()).isFalse();

    //
    //
    fileWorker.start();
    //
    //

    assertThat(parentConfigError.exists()).isFalse();

    List<String> parentLines = parentConfig.readLinesWithoutSpaces();
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

    List<String> hostValues = hostConfig.readLinesWithoutSpaces();
    {
      assertThat(hostValues).contains("con.auto.commit.interval.ms=54267");
      assertThat(hostValues).contains("con.heartbeat.interval.ms=3242456");
      assertThat(hostValues).hasSize(2);
    }

    List<String> actualLines = hostConfigActualValues.readLinesWithoutSpaces();
    {
      assertThat(actualLines).contains("con.auto.commit.interval.ms=54267");
      assertThat(actualLines).contains("con.heartbeat.interval.ms=3242456");
      assertThat(actualLines).hasSize(2);
    }

    assertThat(configDataChanged.happenCount).isZero();

    assertThat(hostConfig.writeContentCount)
      .describedAs("ATTENTION: Host config cannot be touched because it is already exist")
      .isZero();

    assertThat(parentConfig.writeContentCount).isEqualTo(1);

  }

  @Test
  public void inheritance() {

    parentConfig.addLines("con.value.1 = 3333");
    parentConfig.addLines("con.value.2 = 666666666");

    parentConfig.addLines("out.value.1 = 76538");
    parentConfig.addLines("out.value.2 = 3215");

    parentConfig.addLines("out.worker.count=177");

    hostConfig.addLines("con.value.1:inherits");
    hostConfig.addLines("con.value.2=43333333");

    hostConfig.addLines("out.value.1 = 8764990578");
    hostConfig.addLines("out.value.2:inherits");

    hostConfig.addLines("out.worker.count = 711");

    assertThat(parentConfig.exists()).isFalse();

    fileWorker.start();

    assertThat(parentConfigError.exists()).isFalse();

    List<String> actualLines = hostConfigActualValues.readLinesWithoutSpaces();
    {
      assertThat(actualLines).contains("con.value.1=3333");
      assertThat(actualLines).contains("con.value.2=43333333");
      assertThat(actualLines).contains("out.value.1=8764990578");
      assertThat(actualLines).contains("out.value.2=3215");
      assertThat(actualLines).contains("out.value.2=3215");
      assertThat(actualLines).contains("out.worker.count=711");
      assertThat(actualLines).hasSize(5);
    }

    Map<String, Object> configMap = fileWorker.getConfigMap();
    assertThat(configMap).contains(MapEntry.entry("value.1", "3333"));
    assertThat(configMap).contains(MapEntry.entry("value.2", "43333333"));

    assertThat(configDataChanged.happenCount).isZero();
    assertThat(hostConfig.writeContentCount).isZero();
    assertThat(parentConfig.writeContentCount).isZero();
    assertThat(hostConfigActualValues.writeContentCount).isEqualTo(1);
  }

  @Test
  public void getWorkerCount_direct() {

    parentConfig.addLines("out.worker.count=56427");

    hostConfig.addLines("out.worker.count=325166");

    parentConfig.writeContentCount = 0;
    hostConfig.writeContentCount = 0;

    assertThat(parentConfig.exists()).isFalse();

    //
    //
    fileWorker.start();
    //
    //

    assertThat(fileWorker.getWorkerCount()).isEqualTo(325166);

  }

  @Test
  public void getWorkerCount_parent() {

    parentConfig.addLines("out.worker.count=56427");

    hostConfig.addLines("out.worker.count : inherits");

    parentConfig.writeContentCount = 0;
    hostConfig.writeContentCount = 0;

    assertThat(parentConfig.exists()).isFalse();

    //
    //
    fileWorker.start();
    //
    //

    assertThat(fileWorker.getWorkerCount()).isEqualTo(56427);

  }

  @Test
  public void getWorkerCount_noValue() {

    parentConfig.addLines("out.worker.count=56427");

    hostConfig.addLines("out.asd = some value");

    parentConfig.writeContentCount = 0;
    hostConfig.writeContentCount = 0;

    assertThat(parentConfig.exists()).isFalse();

    //
    //
    fileWorker.start();
    //
    //

    assertThat(fileWorker.getWorkerCount()).isEqualTo(0);

    assertThat(hostConfigError.exists()).isTrue();

    printFile("hostConfig", hostConfig);
    printFile("hostConfigError", hostConfigError);

    List<String> errorLines = hostConfigError.readLines();

    assertThat(errorLines.get(1)).isEqualTo("ERROR: Parameter `out.worker.count` is absent - using value `0`");

  }

  @Test
  public void errorValueInHostConfig() {

    parentConfig.addLines("out.worker.count=56427");

    hostConfig.addLines("#some comment1");
    hostConfig.addLines("#some comment2");
    hostConfig.addLines("#some comment3");
    hostConfig.addLines("out.worker.count = 17");
    hostConfig.addLines("  con.session.timeout.ms = left value ");

    assertThat(parentConfig.exists()).isFalse();

    //
    //
    fileWorker.start();
    //
    //

    assertThat(fileWorker.getConfigMap()).doesNotContainKey("con.session.timeout.ms");

    assertThat(hostConfigError.exists()).isTrue();

    printFile("hostConfig", hostConfig);
    printFile("hostConfigError", hostConfigError);

    List<String> errorLines = hostConfigError.readLines();

    assertThat(errorLines.get(1)).isEqualTo(
      "ERROR: line 5, parameter `con.session.timeout.ms`, value `left value` : value must be integer"
    );

  }


  @Test
  public void errorValueInParentConfig() {

    parentConfig.addLines("out.worker.count=56427");
    parentConfig.addLines("# line comment 2");
    parentConfig.addLines("# line comment 3");
    parentConfig.addLines("# line comment 4");
    parentConfig.addLines("# line comment 5");
    parentConfig.addLines("  con.max.poll.records = left VAL    ");

    hostConfig.addLines("out.worker.count = 17");

    parentConfig.writeContentCount = 0;
    hostConfig.writeContentCount = 0;

    assertThat(parentConfig.exists()).isFalse();

    //
    //
    fileWorker.start();
    //
    //

    assertThat(fileWorker.getConfigMap()).doesNotContainKey("con.max.poll.records");

    assertThat(parentConfigError.exists()).isTrue();

    printFile("parentConfig", parentConfig);
    printFile("parentConfigError", parentConfigError);

    List<String> errorLines = parentConfigError.readLines();

    assertThat(errorLines.get(1)).isEqualTo(
      "ERROR: line 6, parameter `con.max.poll.records`, value `left VAL` : value must be integer"
    );

  }

  @Test
  public void noErrorFilesIfNoErrors() {

    parentConfig.addLines("out.worker.count=56427");

    hostConfig.addLines("out.worker.count = 17");

    //
    //
    fileWorker.start();
    //
    //

    assertThat(parentConfigError.exists()).isFalse();
    assertThat(hostConfigError.exists()).isFalse();

  }

  @Test
  public void errorsInBothFiles() {

    parentConfig.addLines("out.worker.count=56427");
    parentConfig.addLines("con.max.poll.records = left");

    hostConfig.addLines("out.worker.count = 17");
    hostConfig.addLines("con.max.poll.records = left out");

    //
    //
    fileWorker.start();
    //
    //

    assertThat(parentConfigError.exists()).isTrue();
    assertThat(hostConfigError.exists()).isTrue();

  }

  @Test
  public void errorsInParent() {

    parentConfig.addLines("out.worker.count=56427");
    parentConfig.addLines("con.max.poll.records = left");

    hostConfig.addLines("out.worker.count = 17");

    //
    //
    fileWorker.start();
    //
    //

    assertThat(parentConfigError.exists()).isTrue();
    assertThat(hostConfigError.exists()).isFalse();

  }

  @Test
  public void errorsInHost() {

    parentConfig.addLines("out.worker.count=56427");
    parentConfig.addLines("con.max.poll.records = 2200");

    hostConfig.addLines("out.worker.count = 17");
    hostConfig.addLines("con.max.poll.records = left out");

    //
    //
    fileWorker.start();
    //
    //

    assertThat(parentConfigError.exists()).isFalse();
    assertThat(hostConfigError.exists()).isTrue();

    assertThat(fileWorker.getConfigMap()).doesNotContainKey("max.poll.records");

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

    //Начальное состояние файлов

    parentConfig.addLines("con.session.timeout.ms = 44444");
    parentConfig.addLines("con.max.poll.interval.ms = 77777");
    parentConfig.addLines("out.worker.count = 37");

    hostConfig.addLines("con.fetch.max.wait.ms = 111");
    hostConfig.addLines("con.send.buffer.bytes = 222");
    hostConfig.addLines("out.worker.count : inherits");
    hostConfig.addLines("con.max.poll.interval.ms : inherits");

    // Запускается система

    //
    //
    fileWorker.start();
    //
    //

    // Проверяем, что всё прочиталось

    {
      int workerCount = fileWorker.getWorkerCount();
      assertThat(workerCount).isEqualTo(37);

      String maxPollIntervalMs = (String) fileWorker.getConfigMap().get("max.poll.interval.ms");
      assertThat(maxPollIntervalMs).isEqualTo("77777");
    }

    // Теперь меняем конфиги

    parentConfig.delLines("con.max.poll.interval.ms = 77777");
    parentConfig.addLines("con.max.poll.interval.ms = 454545");

    parentConfig.delLines("out.worker.count = 37");
    parentConfig.addLines("out.worker.count = 987");

    parentConfig.fireEvent(ConfigEventType.UPDATE);

    // И смотрим, что система обновила значения на новые из конфигов

    {
      int workerCount = fileWorker.getWorkerCount();
      assertThat(workerCount).isEqualTo(987);

      String maxPollIntervalMs = (String) fileWorker.getConfigMap().get("max.poll.interval.ms");
      assertThat(maxPollIntervalMs).isEqualTo("454545");
    }

    // Ну и проверим, что хэндлер отработал

    assertThat(configDataChanged.happenCount).isEqualTo(1);

    // Ошибок не должно быть - проверяем это

    assertThat(parentConfigError.exists()).isFalse();
    assertThat(hostConfigError.exists()).isFalse();
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

    //Начальное состояние файлов-конфигов

    parentConfig.addLines("con.session.timeout.ms = 44444");
    parentConfig.addLines("con.max.poll.interval.ms = 77777");
    parentConfig.addLines("out.worker.count = 0");

    hostConfig.addLines("con.send.buffer.bytes = 222");
    hostConfig.addLines("out.worker.count = 37");

    // Запускается система

    //
    //
    fileWorker.start();
    //
    //

    // Проверяем, что всё прочиталось

    {
      int workerCount = fileWorker.getWorkerCount();
      assertThat(workerCount).isEqualTo(37);

      String sendBufferBytes = (String) fileWorker.getConfigMap().get("send.buffer.bytes");
      assertThat(sendBufferBytes).isEqualTo("222");
    }

    // Теперь меняем конфиги

    hostConfig.delLines("con.send.buffer.bytes = 222");
    hostConfig.addLines("con.send.buffer.bytes = 34565");

    hostConfig.delLines("out.worker.count = 37");
    hostConfig.addLines("out.worker.count = 68451");

    hostConfig.fireEvent(ConfigEventType.UPDATE);

    // И смотрим, что система обновила значения на новые из конфигов

    {
      int workerCount = fileWorker.getWorkerCount();
      assertThat(workerCount).isEqualTo(68451);

      String sendBufferBytes = (String) fileWorker.getConfigMap().get("send.buffer.bytes");
      assertThat(sendBufferBytes).isEqualTo("34565");
    }

    // Ну и проверим, что хэндлер отработал

    assertThat(configDataChanged.happenCount).isEqualTo(1);

    // Ошибок не должно быть - проверяем это

    assertThat(parentConfigError.exists()).isFalse();
    assertThat(hostConfigError.exists()).isFalse();
  }

  /**
   * <p>
   * Нужно проверить, что если вначале файл был без ошибок, а потом ошибки появились - должен создаться файл ошибок
   * </p>
   * <p>
   * Проверяем родительский конфиг-файл
   * </p>
   */
  @Test
  public void checkCreatesErrorFileAfterBadUpdate_parent() {
    // Вначале в файле-конфиге ошибок нет

    parentConfig.addLines("con.session.timeout.ms = 44444");
    parentConfig.addLines("out.worker.count = 37");

    hostConfig.addLines("con.session.timeout.ms : inherits");
    hostConfig.addLines("out.worker.count : inherits");

    // Стартуем приложение

    //
    //
    fileWorker.start();
    //
    //

    // Ошибок нет - смотрим, что файла тоже нет

    assertThat(parentConfigError.exists()).isFalse();

    // Меняем файл, делая в нём ошибку

    parentConfig.delLines("con.session.timeout.ms = 44444");
    parentConfig.addLines("con.session.timeout.ms = err");

    parentConfigError.writeContentCount = 0;

    parentConfig.fireEvent(ConfigEventType.UPDATE);

    // Смотрим, что появился файл ошибок

    assertThat(parentConfigError.exists()).isTrue();

    printFile("parentConfig", parentConfig);
    printFile("parentConfigError", parentConfigError);

    List<String> errorLines = parentConfigError.readLines();
    assertThat(errorLines.get(1)).isEqualTo(
      "ERROR: line 2, parameter `con.session.timeout.ms`, value `err` : value must be integer"
    );

    // Ну и проверим, что хэндлер отработал

    assertThat(configDataChanged.happenCount).isEqualTo(1);

    // Смотрим, чтобы запись в файл была всего одна

    assertThat(parentConfigError.writeContentCount).isEqualTo(1);

  }

  private void printFile(String fileName, TestEventConfigFile file) {
    System.out.println("BEGIN file " + fileName);
    List<String> lines = file.readLines();
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
   * <p>
   * Нужно проверить, что если вначале файл был без ошибок, а потом ошибки появились - должен создаться файл ошибок
   * </p>
   * <p>
   * Проверяем хостовый конфиг-файл
   * </p>
   */
  @Test
  public void checkCreatesErrorFileAfterBadUpdate_host() {
    // Вначале в файле-конфиге ошибок нет

    parentConfig.addLines("con.session.timeout.ms = 44444");
    parentConfig.addLines("out.worker.count = 37");

    hostConfig.addLines("con.session.timeout.ms : inherits");
    hostConfig.addLines("out.worker.count : inherits");

    // Стартуем приложение

    //
    //
    fileWorker.start();
    //
    //

    // Ошибок нет - смотрим, что файла тоже нет

    assertThat(hostConfigError.exists()).isFalse();

    // Меняем файл, делая в нём ошибку

    hostConfig.delLines("con.session.timeout.ms = 44444");
    hostConfig.addLines("con.session.timeout.ms = err");

    hostConfigError.writeContentCount = 0;

    hostConfig.fireEvent(ConfigEventType.UPDATE);

    // Смотрим, что появился файл ошибок

    assertThat(hostConfigError.exists()).isTrue();

    printFile("hostConfig", hostConfig);
    printFile("hostConfigError", hostConfigError);

    List<String> errorLines = hostConfigError.readLines();

    assertThat(errorLines.get(1)).isEqualTo(
      "ERROR: line 3, parameter `con.session.timeout.ms`, value `err` : value must be integer"
    );

    // Ну и проверим, что хэндлер отработал

    assertThat(configDataChanged.happenCount).isEqualTo(1);

    // Смотрим, чтобы запись в файл была всего одна

    assertThat(hostConfigError.writeContentCount).isEqualTo(1);

  }


  /**
   * <p>
   * Нужно проверить, что файл ошибок удаляется, если ошибки исправлены
   * </p>
   * <p>
   * Проверяем родительский конфиг-файл
   * </p>
   */
  @Test
  public void errorFilesDeletesAfterErrorDeleted_parents() {
    // В начале в файлах-конфигах делаем ошибки

    parentConfig.addLines("con.session.timeout.ms = left");
    parentConfig.addLines("out.worker.count = 11");

    hostConfig.addLines("con.session.timeout.ms : inherits");
    hostConfig.addLines("out.worker.count : inherits");

    // Стартуем приложение

    //
    //
    fileWorker.start();
    //
    //

    // Должен быть файл ошибок - проверим это

    assertThat(parentConfigError.exists()).isTrue();

    // Меняем файл, исправляя ошибки

    parentConfig.delLines("con.session.timeout.ms : left");
    parentConfig.addLines("con.session.timeout.ms : inherits");

    parentConfigError.writeContentCount = 0;

    parentConfig.fireEvent(ConfigEventType.UPDATE);

    // Смотрим, что файл ошибок исчез

    assertThat(parentConfigError.exists()).isTrue();

    // Ну и проверим, что хэндлер отработал

    assertThat(configDataChanged.happenCount).isEqualTo(1);

    // Смотрим, чтобы запись в файл была всего одна

    assertThat(parentConfigError.writeContentCount).isEqualTo(1);
  }

  /**
   * <p>
   * Нужно проверить, что файл ошибок удаляется, если ошибки исправлены
   * </p>
   * <p>
   * Проверяем хостовый конфиг-файл
   * </p>
   */
  @Test
  public void errorFilesDeletesAfterErrorDeleted_host() {
    // В начале в файлах-конфигах делаем ошибки

    parentConfig.addLines("con.session.timeout.ms = 45");
    parentConfig.addLines("out.worker.count = 11");

    hostConfig.addLines("con.session.timeout.ms : left");
    hostConfig.addLines("out.worker.count : inherits");

    // Стартуем приложение

    //
    //
    fileWorker.start();
    //
    //

    // Должен быть файл ошибок - проверим это

    assertThat(hostConfigError.exists()).isTrue();

    // Меняем файл, исправляя ошибки

    hostConfig.delLines("con.session.timeout.ms : left");
    hostConfig.addLines("con.session.timeout.ms : inherits");

    hostConfigError.writeContentCount = 0;
    hostConfig.fireEvent(ConfigEventType.UPDATE);

    // Смотрим, что файл ошибок исчез

    printFile("hostConfig", hostConfig);
    if (hostConfigError.exists()) {
      printFile("hostConfigError", hostConfigError);
    }

    assertThat(hostConfigError.exists()).isFalse();

    // Ну и проверим, что хэндлер отработал

    assertThat(configDataChanged.happenCount).isEqualTo(1);

    // Смотрим, чтобы запись в файл была всего одна

    assertThat(hostConfigError.writeContentCount).isEqualTo(1);
  }

}
