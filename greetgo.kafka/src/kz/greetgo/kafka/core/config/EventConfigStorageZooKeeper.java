package kz.greetgo.kafka.core.config;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public class EventConfigStorageZooKeeper extends EventConfigStorageAbstract implements AutoCloseable {

  private final Supplier<String> zookeeperServers;
  private final String rootPath;
  private final IntSupplier sessionTimeout;

  private final AtomicReference<ZooKeeper> zkHolder = new AtomicReference<>(null);

  public EventConfigStorageZooKeeper(String rootPath, Supplier<String> zookeeperServers, IntSupplier sessionTimeout) {
    this.zookeeperServers = zookeeperServers;
    this.rootPath = rootPath;
    this.sessionTimeout = sessionTimeout;
  }

  public void reset() {
    ZooKeeper current = zkHolder.getAndSet(null);
    if (current != null) {
      try {
        current.close();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public ZooKeeper zk() {
    if (!opened.get()) {
      throw new RuntimeException("ConfigStorageZooKeeper closed");
    }
    try {

      {
        ZooKeeper zk = zkHolder.get();
        if (zk != null) {

          if (zk.getState().isAlive()) {
            return zk;
          }

          zkHolder.set(null);
        }
      }

      final ZooKeeper newZK;

      synchronized (zkHolder) {

        {
          ZooKeeper zk = zkHolder.get();
          if (zk != null && zk.getState().isAlive()) {
            return zk;
          }
        }

        final CountDownLatch connSignal = new CountDownLatch(0);
        newZK = new ZooKeeper(zookeeperServers.get(), sessionTimeout.getAsInt(), (WatchedEvent event) -> {
          if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            connSignal.countDown();
          }
        });

        connSignal.await();

        zkHolder.set(newZK);

      }

      prepareWatchers(newZK);

      return newZK;


    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final AtomicBoolean opened = new AtomicBoolean(true);

  @Override
  public void close() {
    opened.set(false);
    reset();
  }

  private String slashRootPath() {
    if (rootPath == null) {
      return "/";
    }
    if (rootPath.startsWith("/")) {
      return rootPath;
    }
    return "/" + rootPath;
  }

  private String zNode(String path) {

    String slashPath = path == null ? "/" : (path.startsWith("/") ? path : "/" + path);

    if (rootPath == null) {
      return slashPath;
    }

    String slashRootPath = slashRootPath();
    if (slashRootPath.endsWith("/")) {
      return slashRootPath + slashPath.substring(1);
    } else {
      return slashRootPath + slashPath;
    }

  }

  private String zNodeToPath(String zNode) {
    if (rootPath == null) {
      if (zNode == null || zNode.isEmpty()) {
        return null;
      }
      if (zNode.startsWith("/")) {
        return zNode.substring(1);
      }
      return zNode;
    }

    String slashRootPath = slashRootPath();

    if (zNode == null || zNode.isEmpty()) {
      return null;
    }

    if (slashRootPath.equals(zNode)) {
      return slashRootPath;
    }

    if (!zNode.startsWith(slashRootPath + "/")) {
      return null;
    }

    return zNode.substring(slashRootPath.length() + 1);
  }

  @Override
  public boolean exists(String path) {

    try {

      return null != zk().exists(zNode(path), null);

    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public byte[] readContent(String path) {

    ZooKeeper zk = zk();
    String zNode = zNode(path);

    try {
      Stat stat = zk.exists(zNode, null);
      if (stat == null) {
        return null;
      }
      return zk.getData(zNode, null, stat);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void writeContent(String path, byte[] content) {

    byte[] current = readContent(path);

    if (Arrays.equals(current, content)) {
      return;
    }

    ZooKeeper zk = zk();
    String zNode = zNode(path);

    try {

      if (content == null) {

        Stat stat = zk.exists(zNode, null);
        if (stat == null) {
          return;
        }

        nodesData.remove(path);

        zk.delete(zNode, stat.getVersion());

      } else {

        Stat stat = zk.exists(zNode, null);
        if (stat == null) {

          nodesData.put(path, content);

          zk.create(zNode, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {

          nodesData.put(path, content);

          zk.setData(zNode, content, stat.getVersion());
        }

      }

    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }


  }

  private final ConcurrentHashMap<String, ZooKeeper> lookingForMap = new ConcurrentHashMap<>();

  private void prepareWatchers(ZooKeeper newZK) {

    List<String> zNodes = new ArrayList<>(lookingForMap.keySet());

    for (String zNode : zNodes) {
      installWatcherOn(newZK, zNode);
    }

  }

  private void installWatcherOn(ZooKeeper zk, String zNode) {
    lookingForMap.put(zNode, zk);

    System.out.println("h32bb4 :: installing watcher on " + zNode);

    try {
      zk.exists(zNode, this::processEvent);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static ConfigEventType eventTypeToType(Watcher.Event.EventType type) {
    if (type == null) {
      return null;
    }
    switch (type) {
      case NodeCreated:
        return ConfigEventType.CREATE;
      case NodeDataChanged:
        return ConfigEventType.UPDATE;
      case NodeDeleted:
        return ConfigEventType.DELETE;
      default:
        return null;
    }
  }

  @Override
  public void ensureLookingFor(String path) {
    ZooKeeper zk = zk();
    String zNode = zNode(path);

    if (zk == lookingForMap.get(zNode)) {
      return;
    }

    nodesData.put(path, readContent(path));

    installWatcherOn(zk, zNode);
  }


  private void processEvent(WatchedEvent event) {

    System.out.println("jn2j4n :: processEvent : event = " + event);

    String zNode = event.getPath();

    String path = zNodeToPath(zNode);

    if (path == null) {
      return;
    }

    ConfigEventType eventType = eventTypeToType(event.getType());
    if (eventType == null) {
      return;
    }

    if (opened.get() && lookingForMap.containsKey(zNode)) {
      ZooKeeper zk = zk();
      installWatcherOn(zk, zNode);
    }

    fireConfigEventHandlerLocal(path, eventType);

  }

  private final ConcurrentHashMap<String, byte[]> nodesData = new ConcurrentHashMap<>();

  private void fireConfigEventHandlerLocal(String path, ConfigEventType eventType) {

    System.out.println("3j24jn5 :: fireConfigEventHandlerLocal : " + path + " " + eventType);

    if (eventType == ConfigEventType.CREATE || eventType == ConfigEventType.UPDATE) {

      while (true) {
        byte[] current = readContent(path);
        if (current == null) {
          return;
        }
        byte[] cached = nodesData.get(path);
        if (Arrays.equals(current, cached)) {
          return;
        }
        if (nodesData.replace(path, cached, current)) {
          fireConfigEventHandler(path, eventType);
          return;
        }
      }

    }

    if (eventType == ConfigEventType.DELETE) {
      while (true) {
        byte[] current = readContent(path);
        if (current != null) {
          return;
        }

        byte[] cached = nodesData.get(path);
        if (cached == null) {
          return;
        }

        if (nodesData.remove(path, cached)) {
          fireConfigEventHandler(path, eventType);
        }
      }
    }

  }
}
