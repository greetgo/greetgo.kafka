package kz.greetgo.kafka.core.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public class EventConfigStorageZooKeeper extends EventConfigStorageAbstract implements AutoCloseable {

  private final Supplier<String> zookeeperServers;
  private final String rootPath;
  private final IntSupplier sessionTimeout;

  private final AtomicReference<ZooKeeper> zkHolder = new AtomicReference<>(null);

  private final AtomicReference<CuratorFramework> clientHolder = new AtomicReference<>(null);

  public EventConfigStorageZooKeeper(String rootPath, Supplier<String> zookeeperServers, IntSupplier sessionTimeout) {
    this.zookeeperServers = zookeeperServers;
    this.rootPath = rootPath;
    this.sessionTimeout = sessionTimeout;
  }

  public void reset() {
    {
      ZooKeeper current = zkHolder.getAndSet(null);
      if (current != null) {
        try {
          current.close();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    {
      CuratorFramework current = clientHolder.getAndSet(null);
      if (current != null) {
        current.close();
      }
    }
  }

  public CuratorFramework client() {
    if (!opened.get()) {
      throw new RuntimeException(getClass().getSimpleName() + " closed");
    }
    return clientHolder.accumulateAndGet(null, (current, ignore) -> current != null ? current : createClient());
  }

  private CuratorFramework createClient() {
    int sleepMsBetweenRetries = 100;
    int maxRetries = 3;
    RetryPolicy retryPolicy = new RetryNTimes(
      maxRetries, sleepMsBetweenRetries);

    CuratorFramework client = CuratorFrameworkFactory
      .newClient(zookeeperServers.get(), sessionTimeout.getAsInt(), 25000, retryPolicy);

    client.start();

    prepareWatchers(client);

    return client;
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
  public Optional<Date> createdAt(String path) {

    try {

      return Optional.ofNullable(

        client().checkExists().forPath(zNode(path))

      ).map(Stat::getCtime).map(Date::new);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public Optional<Date> lastModifiedAt(String path) {

    try {

      return Optional.ofNullable(

        client().checkExists().forPath(zNode(path))

      ).map(Stat::getMtime).map(Date::new);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public byte[] readContent(String path) {

    CuratorFramework client = client();
    String zNode = zNode(path);

    try {

      client.checkExists().forPath(zNode);

      Stat stat = client.checkExists().forPath(zNode);
      if (stat == null) {
        return null;
      }

      return client.getData().forPath(zNode);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void writeContent(String path, byte[] content) {

    byte[] current = readContent(path);

    if (Arrays.equals(current, content)) {
      return;
    }

    CuratorFramework client = client();
    String zNode = zNode(path);

    try {

      if (content == null) {


        Stat stat = client.checkExists().forPath(zNode);
        if (stat == null) {
          return;
        }

        nodesData.remove(path);

        client.delete().withVersion(stat.getVersion()).forPath(zNode);

      } else {

        Stat stat = client.checkExists().forPath(zNode);
        if (stat == null) {

          nodesData.put(path, content);

          client.create().creatingParentContainersIfNeeded().forPath(zNode, content);

        } else {

          nodesData.put(path, content);

          client.setData().withVersion(stat.getVersion()).forPath(zNode, content);
        }

      }


    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final ConcurrentHashMap<String, CuratorFramework> lookingForMap = new ConcurrentHashMap<>();

  private void prepareWatchers(CuratorFramework newClient) {

    List<String> zNodes = new ArrayList<>(lookingForMap.keySet());

    for (String zNode : zNodes) {
      installWatcherOn(newClient, zNode);
    }

  }

  private void installWatcherOn(CuratorFramework client, String zNode) {
    lookingForMap.put(zNode, client);

    try {
      client.checkExists().usingWatcher((CuratorWatcher) this::processEvent).forPath(zNode);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
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

    if (path == null) {
      throw new IllegalArgumentException("path == null");
    }

    CuratorFramework client = client();
    String zNode = zNode(path);

    if (client == lookingForMap.get(zNode)) {
      return;
    }

    {
      byte[] content = readContent(path);
      if (content == null) {
        nodesData.remove(path);
      } else {
        nodesData.put(path, content);
      }
    }

    installWatcherOn(client, zNode);
  }

  private void processEvent(WatchedEvent event) {

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
      installWatcherOn(client(), zNode);
    }

    fireConfigEventHandlerLocal(path, eventType);

  }

  private final ConcurrentHashMap<String, byte[]> nodesData = new ConcurrentHashMap<>();

  private void fireConfigEventHandlerLocal(String path, ConfigEventType eventType) {

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
