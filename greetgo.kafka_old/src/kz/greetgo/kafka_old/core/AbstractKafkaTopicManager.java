package kz.greetgo.kafka_old.core;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.api.OffsetFetchRequest;
import kafka.api.OffsetFetchResponse;
import kafka.api.OffsetRequest;
import kafka.api.OffsetResponse;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.PartitionOffsetsResponse;
import kafka.client.ClientUtils;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.network.BlockingChannel;
import kafka.utils.ZKGroupDirs;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kz.greetgo.kafka_old.core.model.GroupIdInstance;
import kz.greetgo.kafka_old.core.model.GroupIdInstanceSizeOffset;
import kz.greetgo.kafka_old.core.model.SizeOffset;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.Map$;
import scala.collection.mutable.WrappedArray;
import scala.util.parsing.json.JSON;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

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

  public List<GroupIdInstance> allGroupIdInstances() {
    List<GroupIdInstance> ret = new ArrayList<>();
    try (ZkClientHolder holder = createKzClient()) {

      ZkUtils zkUtils = new ZkUtils(holder.client, holder.connection, false);

      Seq<String> consumerGroups = zkUtils.getConsumerGroups();
      List<String> consumerGroupsList = JavaConversions.seqAsJavaList(consumerGroups);

      for (String group : consumerGroupsList) {
        GroupIdInstance instance = new GroupIdInstance();
        instance.groupId = group;
        ret.add(instance);
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return ret;
  }

  static class GetSizer implements Closeable {
    private java.util.Map<String, SimpleConsumer> simpleConsumerMap = new java.util.HashMap<>();

    public long countOffsetCount(ZkUtils zkUtils, TopicAndPartition topicAndPartition, ZKGroupTopicDirs zkGroupTopicDirs) {
      try {
        if (!zkUtils.pathExists(zkGroupTopicDirs.consumerOffsetDir())) return 0;
        if (!zkUtils.pathExists(zkGroupTopicDirs.consumerOffsetDir() + "/" + "%d".format(topicAndPartition.partition() + "")))
          return 0;

        String s = zkUtils.readData(zkGroupTopicDirs.consumerOffsetDir() + "/" + "%d".format(topicAndPartition.partition() + ""))._1;

        return Long.parseLong(s);

      } catch (ZkNoNodeException zkNoNodeException) {
        zkNoNodeException.printStackTrace();
        return 0;
      }

    }

    public Future<Long> getSize(ZkUtils zkUtils, TopicAndPartition topicAndPartition) {
      return ForkJoinPool.commonPool().submit(() -> countLogCount(zkUtils, topicAndPartition));
    }


    public long countLogCount(ZkUtils zkUtils, TopicAndPartition topicAndPartition) {
      try {
        Option<Object> leaderForPartition = zkUtils.getLeaderForPartition(topicAndPartition.topic(), topicAndPartition.partition());
        if (leaderForPartition == null) return 0;

        if (!zkUtils.pathExists("/brokers")) return 0;
        if (!zkUtils.pathExists("/brokers/ids")) return 0;
        if (!zkUtils.pathExists("/brokers/ids/" + (Integer) leaderForPartition.get())) return 0;

        Option<String> stringOption = zkUtils.readDataMaybeNull("/brokers/ids/" + (Integer) leaderForPartition.get())._1;
        if (stringOption == null) return 0;

        Option<Object> objectOption = JSON.parseFull(stringOption.get());

        if (objectOption == null) return 0;
        java.util.Map map = JavaConversions.mapAsJavaMap((scala.collection.immutable.Map) objectOption.get());
        if (map.isEmpty()) return 0;
        String host = (String) map.get("host");
        if (host == null) return 0;
        Integer port = (Integer) map.get("port");
        if (port == null) return 0;

        SimpleConsumer getLogCount = simpleConsumerMap.get(host + "_" + port);

        if (getLogCount == null) {
          simpleConsumerMap.put(host + "_" + port, new SimpleConsumer(host, port, 10000, 10000, "GetLogCount"));
          getLogCount = simpleConsumerMap.get(host + "_" + port);
        }

        final Tuple2[] ts = {new Tuple2<>(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1))};
        final WrappedArray wa = Predef.wrapRefArray(ts);
        scala.collection.immutable.Map<TopicAndPartition, PartitionOffsetRequestInfo> partitionOffsetRequestInfoMap =
          Map$.MODULE$.apply(wa);

        OffsetResponse offsetsBefore = getLogCount.getOffsetsBefore(new OffsetRequest(partitionOffsetRequestInfoMap, 0, -1));

        scala.collection.immutable.Map<TopicAndPartition, PartitionOffsetsResponse> topicAndPartitionPartitionOffsetsResponseMap =
          offsetsBefore.partitionErrorAndOffsets();
        Option<PartitionOffsetsResponse> partitionOffsetsResponseOption = topicAndPartitionPartitionOffsetsResponseMap.get(topicAndPartition);
        PartitionOffsetsResponse partitionOffsetsResponse = partitionOffsetsResponseOption.get();
        Seq<Object> offsets = partitionOffsetsResponse.offsets();

        return (long) offsets.head();
      } catch (ZkNoNodeException zkNoNodeException) {
        zkNoNodeException.printStackTrace();
        return 0;
      }
    }

    @Override
    public void close() {
      for (String id : simpleConsumerMap.keySet()) {
        SimpleConsumer simpleConsumer = simpleConsumerMap.get(id);
        simpleConsumer.close();
      }
    }
  }

  public List<GroupIdInstanceSizeOffset> sizeOffsets(List<GroupIdInstance> input) {

    List<GroupIdInstanceSizeOffset> ret = new ArrayList<>();

    try (ZkClientHolder holder = createKzClient()) {
      try (GetSizer getSizer = new GetSizer()) {
        for (GroupIdInstance instance : input) {
          if (instance.groupId == null) continue;

          ZkUtils zkUtils = new ZkUtils(holder.client, holder.connection, false);
          ZKGroupDirs groupDirs = new ZKGroupDirs(instance.groupId);

          if (!zkUtils.pathExists(groupDirs.consumerGroupDir()))
            if (!zkUtils.pathExists(groupDirs.consumerGroupDir() + "/owners")) continue;

          scala.collection.immutable.List<String> topicsList = zkUtils.getChildren(groupDirs.consumerGroupDir() + "/owners").toList();
          java.util.Map<String, Seq<Object>> partitionsForTopics = JavaConversions.mapAsJavaMap(zkUtils.getPartitionsForTopics(topicsList));

          List<TopicAndPartition> topicAndPartitions = new ArrayList<>();
          for (String s : partitionsForTopics.keySet()) {
            Seq<Object> partitions = partitionsForTopics.get(s);

            List<Object> objects = JavaConversions.seqAsJavaList(partitions);
            for (Object object : objects) {
              topicAndPartitions.add(new TopicAndPartition(s, (Integer) object));
            }
          }

          Seq<TopicAndPartition> topicAndPartitionSeq = JavaConversions.asScalaBuffer(topicAndPartitions).toSeq();

          BlockingChannel blockingChannel = ClientUtils.channelToOffsetManager(instance.groupId, zkUtils, 6000, 3000);

          blockingChannel.send(new OffsetFetchRequest(instance.groupId, topicAndPartitionSeq, OffsetFetchRequest.CurrentVersion(), 0, ""));

          OffsetFetchResponse offsetFetchResponse = OffsetFetchResponse.readFrom(blockingChannel.receive().payload());

          java.util.Map<TopicAndPartition, OffsetMetadataAndError> topicAndPartitionOffsetMetadataAndErrorMap1 = JavaConversions.mapAsJavaMap(offsetFetchResponse.requestInfo());

          long offsetCount = 0;
          long logCount = 0;

          List<Future<Long>> sizeList = new ArrayList<>();

          for (TopicAndPartition topicAndPartition : topicAndPartitionOffsetMetadataAndErrorMap1.keySet()) {

            ZKGroupTopicDirs zkGroupTopicDirs = new ZKGroupTopicDirs(instance.groupId, topicAndPartition.topic());

            if (!zkUtils.pathExists(zkGroupTopicDirs.consumerDir())) continue;

            offsetCount += getSizer.countOffsetCount(zkUtils, topicAndPartition, zkGroupTopicDirs);

            sizeList.add(getSizer.getSize(zkUtils, topicAndPartition));

          }

          for (Future<Long> sizeFuture : sizeList) {
            logCount += sizeFuture.get();
          }

          GroupIdInstanceSizeOffset offset = new GroupIdInstanceSizeOffset();
          offset.groupIdInstance = instance;
          offset.sizeOffset = new SizeOffset();

          offset.sizeOffset.offset = offsetCount;
          offset.sizeOffset.size = logCount;

          ret.add(offset);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return ret;
  }

}
  

