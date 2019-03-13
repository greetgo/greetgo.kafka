package kz.greetgo.kafka2.core.config;

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

public class ConfigStorageZooKeeper extends ConfigStorageAbstract implements AutoCloseable {

  private final String zookeeperServers;
  private final String rootPath;

  private final AtomicReference<ZooKeeper> zkHolder = new AtomicReference<>(null);

  public ConfigStorageZooKeeper(String rootPath, String zookeeperServers) {
    this.zookeeperServers = zookeeperServers;
    this.rootPath = rootPath;
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
        newZK = new ZooKeeper(zookeeperServers, 3000, (WatchedEvent event) -> {
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
    ZooKeeper current = zkHolder.getAndSet(null);
    if (current != null) {
      try {
        current.close();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
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

      Stat stat = zk().exists(zNode(path), true);

      return stat != null;

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

        incrementSkipping(path);

        zk.delete(zNode, stat.getVersion());

      } else {

        Stat stat = zk.exists(zNode, null);
        if (stat == null) {

          incrementSkipping(path);

          zk.create(zNode, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {

          incrementSkipping(path);

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
  private final ConcurrentHashMap<String, Integer> skipCount = new ConcurrentHashMap<>();

  private boolean allowFire(String path) {
    return null == skipCount.compute(path, (pathArg, currentCount) -> {
      if (currentCount == null || currentCount == 0) {
        return null;
      }
      return currentCount - 1;
    });
  }

  private void incrementSkipping(String path) {

    Integer result = skipCount.compute(path, (pathArg, currentCount) -> {
      if (currentCount == null) {
        return 1;
      }
      return currentCount + 1;
    });

    System.out.println("km32m4m6 :: incrementSkipping " + path + " to " + result);
  }

  private void fireConfigEventHandlerLocal(String path, ConfigEventType eventType) {

    System.out.println("3j24jn5 :: fireConfigEventHandlerLocal : " + path + " " + eventType);

    if (eventType == ConfigEventType.CREATE) {
      nodesData.put(path, readContent(path));
      if (allowFire(path)) {
        fireConfigEventHandler(path, eventType);
      }
      return;
    }

    if (eventType == ConfigEventType.DELETE) {

      byte[] bytes = nodesData.remove(path);
      if (bytes != null) {
        if (allowFire(path)) {
          fireConfigEventHandler(path, eventType);
        }
      }
      return;
    }

    if (eventType == ConfigEventType.UPDATE) {

      while (true) {
        byte[] cachedBytes = nodesData.get(path);
        byte[] currentBytes = readContent(path);

        if (currentBytes == null) {
          return;
        }

        if (Arrays.equals(cachedBytes, currentBytes)) {
          return;
        }

        if (nodesData.replace(path, cachedBytes, currentBytes)) {
          if (allowFire(path)) {
            fireConfigEventHandler(path, eventType);
          }
          return;
        }

      }

    }
  }
}
