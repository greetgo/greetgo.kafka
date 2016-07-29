package kz.greetgo.kafka.core;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

public abstract class AbstractKafkaTopicManager {

  protected int sessionTimeOut() {
    return 3000;
  }

  protected int connectionTimeout() {
    return 3000;
  }

  private ZkClientHolder createKzClient() {
    ZkConnection zkConnection = new ZkConnection(zookeeperServers(), sessionTimeOut());
    ZkClient zkClient = new ZkClient(zkConnection, connectionTimeout(), ZKStringSerializer$.MODULE$);
    return new ZkClientHolder(zkConnection, zkClient);
  }

  public void createTopic(String topicName, int partitionCount, int replicationFactor) {
    try (ZkClientHolder holder = createKzClient()) {
      ZkUtils zkUtils = new ZkUtils(holder.client, holder.connection, false);
      AdminUtils.createTopic(zkUtils, topicName, partitionCount, replicationFactor, new Properties(),
          RackAwareMode.Safe$.MODULE$);
    } catch (Exception e) {
      if (e instanceof RuntimeException) throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }

  protected abstract String zookeeperServers();

  public void removeTopic(String topicName) {
    try (ZkClientHolder holder = createKzClient()) {

      holder.client.deleteRecursive(ZkUtils.getTopicPath(topicName));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
  

