package kz.greetgo.kafka_old.probes;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;

public class ProbeCreateTopic {
  public static void main(String[] args) {
    TopicCommand.main(new String[]{
        "--create", "--topic", "asd_test",
        "--zookeeper", "localhost:2181",
        "--partitions", "2", "--replication-factor", "1"
    });
    
  }
}
