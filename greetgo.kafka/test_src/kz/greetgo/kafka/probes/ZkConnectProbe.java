package kz.greetgo.kafka.probes;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ZkConnectProbe {
  private ZooKeeper zk;

  public ZooKeeper connect(String host) throws Exception {
    final CountDownLatch connSignal = new CountDownLatch(0);
    zk = new ZooKeeper(host, 3000, (WatchedEvent event) -> {
      if (event.getState() == KeeperState.SyncConnected) {
        connSignal.countDown();
      }
    });
    connSignal.await();
    return zk;
  }

  public void close() throws InterruptedException {
    zk.close();
  }

  public void createNode(String path, byte[] data) throws Exception {
    zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  public void updateNode(String path, byte[] data) throws Exception {
    zk.setData(path, data, zk.exists(path, true).getVersion());
  }

  public void deleteNode(String path) throws Exception {
    zk.delete(path, zk.exists(path, true).getVersion());
  }

  public static void main(String[] args) throws Exception {
    ZkConnectProbe connector = new ZkConnectProbe();
    ZooKeeper zk = connector.connect("localhost:2181");
    String newNode = "/deepakDate" + new Date();
    connector.createNode(newNode, new Date().toString().getBytes());
    List<String> zNodes = zk.getChildren("/", true);
    for (String zNode : zNodes) {
      System.out.println("ChildrenNode " + zNode);
    }
    byte[] data = zk.getData(newNode, true, zk.exists(newNode, true));
    System.out.println("GetData before setting");
    for (byte dataPoint : data) {
      System.out.print((char) dataPoint);
    }

    System.out.println("GetData after setting");
    connector.updateNode(newNode, "Modified data".getBytes());
    data = zk.getData(newNode, true, zk.exists(newNode, true));
    for (byte dataPoint : data) {
      System.out.print((char) dataPoint);
    }


    File workingFile = new File("build/ZkConnectProbe/working.txt");
    workingFile.getParentFile().mkdirs();
    workingFile.createNewFile();

    while (workingFile.exists()) {
      Thread.sleep(800);
    }

    connector.deleteNode(newNode);

    connector.close();
  }

}
