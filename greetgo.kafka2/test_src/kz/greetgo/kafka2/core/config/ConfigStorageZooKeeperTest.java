package kz.greetgo.kafka2.core.config;

import kz.greetgo.kafka2.util.NetUtil;
import org.apache.zookeeper.ZooKeeper;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ConfigStorageZooKeeperTest {

  String zookeeperServers;

  @BeforeMethod
  public void pingKafka() {
    zookeeperServers = "localhost:2181";

    if (!NetUtil.canConnectToAnyBootstrapServer(zookeeperServers)) {
      throw new SkipException("No zookeeper connection : " + zookeeperServers);
    }
  }

  @Test
  public void testStartStop() throws Exception {

    try (ConfigStorageZooKeeper configStorage = new ConfigStorageZooKeeper(
      "test/root", () -> zookeeperServers, () -> 3000)
    ) {

      configStorage.addEventHandler((path, type)
        -> System.out.println("***   ***   ***   : Event happened: " + type + " " + path));

      configStorage.ensureLookingFor("asd.txt");
      configStorage.ensureLookingFor("status.txt");

      ZooKeeper zk = configStorage.zk();
      System.out.println("zk = " + zk);

      String testDir = "build/ConfigStorageZooKeeperTest";
      File workingFile = new File(testDir + "/working.txt");
      File lockWorkingFile = new File(testDir + "/lockWorking.txt");
      if (!lockWorkingFile.exists()) {
        File lockWorkingFile2 = new File(testDir + "/lockWorking__killThisSuffix.txt");
        lockWorkingFile2.getParentFile().mkdirs();
        lockWorkingFile2.createNewFile();
      }

      workingFile.getParentFile().mkdirs();
      workingFile.createNewFile();

      final AtomicBoolean working = new AtomicBoolean(true);

      Thread writeContentThread = new Thread(() -> {
        try {
          writeContentCommandWorking(working, configStorage, testDir);
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      Thread readContentThread = new Thread(() -> {
        try {
          readContentCommandWorking(working, configStorage, testDir);
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      writeContentThread.start();
      readContentThread.start();

      for (int i = 0; workingFile.exists(); i++) {
        Thread.sleep(600);
        if (!lockWorkingFile.exists()) {
          break;
        }
      }

      working.set(false);

      writeContentThread.join();
      readContentThread.join();
    }
  }

  private void readContentCommandWorking(AtomicBoolean working,
                                         ConfigStorageZooKeeper configStorage,
                                         String testDir) throws Exception {

    File cmdFile = new File(testDir + "/readContent.txt");
    File cmdFileOk = new File(testDir + "/readContent-OK.txt");
    cmdFileOk.getParentFile().mkdirs();
    cmdFileOk.createNewFile();

    while (working.get()) {

      if (!cmdFile.exists()) {
        Thread.sleep(800);
        continue;
      }

      List<String> fileLines = Files.readAllLines(cmdFile.toPath());

      if (cmdFileOk.exists()) {
        cmdFileOk.delete();
      }
      cmdFile.renameTo(cmdFileOk);

      for (String line : fileLines) {

        String trimmedLine = line.trim();
        if (trimmedLine.isEmpty()) {
          continue;
        }
        if (trimmedLine.startsWith("#")) {
          continue;
        }

        //noinspection UnnecessaryLocalVariable
        String path = trimmedLine;

        byte[] readContent = configStorage.readContent(path);
        System.out.println("reading " + path);

        //noinspection SpellCheckingInspection
        SimpleDateFormat sdf = new SimpleDateFormat("HHmmss");

        if (readContent == null) {
          String outFileName = cmdFile.getPath() + "-" + sdf.format(new Date()) + "-" + path + ".IS_NULL.txt";
          new File(outFileName).createNewFile();
        } else {
          String outFileName = cmdFile.getPath() + "-" + sdf.format(new Date()) + "-" + path + ".out.txt";

          Files.write(Paths.get(outFileName), readContent);
        }


      }

    }

  }

  private void writeContentCommandWorking(AtomicBoolean working,
                                          ConfigStorageZooKeeper configStorage,
                                          String testDir) throws Exception {

    File cmdFile = new File(testDir + "/writeContent.txt");
    File cmdFileOk = new File(testDir + "/writeContent-OK.txt");
    cmdFileOk.getParentFile().mkdirs();
    cmdFileOk.createNewFile();

    while (working.get()) {

      if (!cmdFile.exists()) {
        Thread.sleep(800);
        continue;
      }

      List<String> fileLines = Files.readAllLines(cmdFile.toPath());

      if (cmdFileOk.exists()) {
        cmdFileOk.delete();
      }
      cmdFile.renameTo(cmdFileOk);

      for (String line : fileLines) {

        String trimmedLine = line.trim();
        if (trimmedLine.isEmpty()) {
          continue;
        }
        if (trimmedLine.startsWith("#")) {
          continue;
        }

        int idx = trimmedLine.indexOf(' ');
        if (idx < 0) {
          continue;
        }

        String path = trimmedLine.substring(0, idx).trim();
        String value = trimmedLine.substring(idx + 1).trim();

        if ("NULL".equals(value)) {
          configStorage.writeContent(path, null);
          System.out.println("Called configStorage.writeContent('" + path + "', null);");
          System.out.flush();
        } else {
          configStorage.writeContent(path, value.getBytes(UTF_8));
          System.out.println("Called configStorage.writeContent('" + path + "', '" + value + "');");
          System.out.flush();
        }
      }

    }
  }
}
