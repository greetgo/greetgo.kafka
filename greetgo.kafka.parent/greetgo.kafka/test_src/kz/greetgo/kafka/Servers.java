package kz.greetgo.kafka;

import kafka.server.KafkaServerStartable;
import kz.greetgo.util.ServerUtil;
import org.apache.kafka.common.utils.Utils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.File;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Servers {
  private static String createTmpDir() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss-SSS");
    return "build/" + sdf.format(new Date());
  }

  final String tmpDir = createTmpDir();
  final int zookeeperClientPort = 2181;
  public final int kafkaServerPort = 9092;

  ServerCnxnFactory zookeeperCnxnFactory = null;
  KafkaServerStartable kafkaServer = null;

  public void startupAll() throws Exception {
    startupZookeeper();
    System.out.println("ZooKeeper started");
    startupKafkaServer();
    System.out.println("Kafka Server started");
  }

  public void shutdownAll() {
    shutdownKafkaServer();
    System.out.println("ZooKeeper downed");
    shutdownZookeeper();
    System.out.println("Kafka Server downed");
  }


  private void startupKafkaServer() throws Exception {
    File kafkaServerConfig = new File(tmpDir + "/kafka_server.config");
    kafkaServerConfig.getParentFile().mkdirs();

    try (PrintStream pr = new PrintStream(kafkaServerConfig, "UTF-8")) {
      pr.println("dataDir=" + tmpDir + "/kafka_server_data");
      pr.println("broker.id=0");
      pr.println("listeners=PLAINTEXT://:" + kafkaServerPort);
      pr.println("num.network.threads=3");
      pr.println("num.io.threads=8");
      pr.println("socket.send.buffer.bytes=102400");
      pr.println("socket.receive.buffer.bytes=102400");
      pr.println("socket.request.max.bytes=104857600");
      pr.println("log.dirs=" + tmpDir + "/kafka_server_data");
      pr.println("num.partitions=1");
      pr.println("num.recovery.threads.per.data.dir=1");
      pr.println("log.retention.hours=168");
      pr.println("log.segment.bytes=1073741824");
      pr.println("log.retention.check.interval.ms=300000");
      pr.println("log.cleaner.enable=false");
      pr.println("zookeeper.connect=localhost:" + zookeeperClientPort);
      pr.println("zookeeper.connection.timeout.ms=6000");
    }

    Properties properties = Utils.loadProps(kafkaServerConfig.getPath());

    kafkaServer = KafkaServerStartable.fromProps(properties);
    kafkaServer.startup();
  }


  private void shutdownKafkaServer() {
    if (kafkaServer != null) {
      kafkaServer.shutdown();
      kafkaServer = null;
    }
  }

  void clean() {
    ServerUtil.deleteRecursively(tmpDir);
  }

  private void shutdownZookeeper() {
    if (zookeeperCnxnFactory != null) {
      zookeeperCnxnFactory.shutdown();
      zookeeperCnxnFactory = null;
    }
  }

  private void startupZookeeper() throws Exception {
    if (zookeeperCnxnFactory != null) throw new RuntimeException();

    File zookeeperConfig = new File(tmpDir + "/zookeeper.config");
    zookeeperConfig.getParentFile().mkdirs();

    try (PrintStream pr = new PrintStream(zookeeperConfig, "UTF-8")) {
      pr.println("dataDir=" + tmpDir + "/zookeeper_data");
      pr.println("clientPort=" + zookeeperClientPort);
      pr.println("maxClientCnxns=0");
    }

    ServerConfig config = new ServerConfig();
    config.parse(zookeeperConfig.getPath());

    zookeeperCnxnFactory = ServerCnxnFactory.createFactory();
    zookeeperCnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());

    FileTxnSnapLog txnLog = new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir()));

    ZooKeeperServer zkServer = new ZooKeeperServer();
    zkServer.setTxnLogFactory(txnLog);
    zkServer.setTickTime(config.getTickTime());
    zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
    zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());

    zookeeperCnxnFactory.startup(zkServer);
  }

}
