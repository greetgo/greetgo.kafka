package kz.greetgo.kafka.core;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;


public class ZkClientHolder implements AutoCloseable {
  public final ZkConnection connection;
  public final ZkClient client;

  public ZkClientHolder(ZkConnection connection, ZkClient zkClient) {
    this.connection = connection;
    this.client = zkClient;
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }
}
