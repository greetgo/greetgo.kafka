package kz.greetgo.kafka_old.probes.more;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

public class CreateTopic {
  public static void main(String[] args) throws Exception {
    ZkConnection zkConnection = new ZkConnection("localhost:2181", 3000);
    try {
      ZkClient zkClient = new ZkClient(zkConnection, 3000, ZKStringSerializer$.MODULE$);
      ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
      AdminUtils.createTopic(zkUtils,
          Params.TOPIC_NAME, Params.TOPIC_PARTITIONS, Params.TOPIC_REPLICATION_FACTOR,
          new Properties(), RackAwareMode.Safe$.MODULE$);
    } finally {
      zkConnection.close();
    }

    System.out.println("Topic " + Params.TOPIC_NAME + " created ok");
  }
}

