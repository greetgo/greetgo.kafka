package kz.greetgo.kafka_old.core.model;

public class GroupIdInstance {
  public String instanceId;
  public String groupId;
  public int partitionIndex;
  public String serverId;

  @Override
  public String toString() {
    return "GroupIdInstance{" +
        "instanceId='" + instanceId + '\'' +
        ", groupId='" + groupId + '\'' +
        ", partitionIndex=" + partitionIndex +
        ", serverId='" + serverId + '\'' +
        '}';
  }
}
