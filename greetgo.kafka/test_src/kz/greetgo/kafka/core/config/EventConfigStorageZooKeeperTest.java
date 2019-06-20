package kz.greetgo.kafka.core.config;

import kz.greetgo.kafka.util.NetUtil;
import kz.greetgo.util.RND;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class EventConfigStorageZooKeeperTest {

  String zookeeperServers;

  @BeforeMethod
  public void pingKafka() {
    zookeeperServers = "localhost:2181";

    if (!NetUtil.canConnectToAnyBootstrapServer(zookeeperServers)) {
      throw new SkipException("No zookeeper connection : " + zookeeperServers);
    }
  }

  @Test
  public void writeContent_readContent_parallel() throws Throwable {

    try (EventConfigStorage configStorage = new EventConfigStorageZooKeeper(
      "test/EventConfigStorageZooKeeperTest", () -> zookeeperServers, () -> 3000)
    ) {

      final String fileName = "rnd-file-name-" + RND.str(10) + ".txt";

      abstract class ThreadWithError extends Thread {
        Throwable error = null;
      }

      class WriteThread extends ThreadWithError {
        @Override
        public void run() {
          try {
            configStorage.writeContent(fileName, RND.str(10).getBytes(UTF_8));
          } catch (Throwable e) {
            error = e;
          }
        }
      }

      class ReadThread extends ThreadWithError {
        @Override
        public void run() {
          try {
            configStorage.readContent(fileName);
          } catch (Throwable e) {
            error = e;
          }
        }
      }

      List<ThreadWithError> threads = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        threads.add(new WriteThread());
      }
      for (int i = 0; i < 10; i++) {
        threads.add(new ReadThread());
      }

      for (ThreadWithError t : threads) {
        t.start();
      }
      for (ThreadWithError t : threads) {
        t.join();
      }

      List<Throwable> list = threads
        .stream()
        .map(t -> t.error)
        .filter(Objects::nonNull)
        .collect(toList());

      if (list.isEmpty()) {
        return;
      }

      Throwable error = list.remove(0);

      for (Throwable throwable : list) {
        throwable.printStackTrace();
      }

      throw error;
    }

  }

}
