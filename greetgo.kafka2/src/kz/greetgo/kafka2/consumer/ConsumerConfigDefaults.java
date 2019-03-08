package kz.greetgo.kafka2.consumer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class ConsumerConfigDefaults {
  public final LinkedHashMap<String, String> patentableValues = new LinkedHashMap<>();
  public final LinkedHashMap<String, String> ownValues = new LinkedHashMap<>();

  public ConsumerConfigDefaults() {
    {
      List<String> lines = new ArrayList<>();

      lines.add("con.auto.commit.interval.ms=1000");
      lines.add("con.session.timeout.ms=30000");
      lines.add("con.heartbeat.interval.ms=10000");
      lines.add("con.fetch.min.bytes=1");
      lines.add("con.max.partition.fetch.bytes=1048576");
      lines.add("con.connections.max.idle.ms=540000");
      lines.add("con.default.api.timeout.ms=60000");
      lines.add("con.fetch.max.bytes=52428800");
      lines.add("con.max.poll.interval.ms=300000");
      lines.add("con.max.poll.records=500");
      lines.add("con.receive.buffer.bytes=65536");
      lines.add("con.request.timeout.ms=30000");
      lines.add("con.send.buffer.bytes=131072");
      lines.add("con.fetch.max.wait.ms=500");

      appendLines(patentableValues, lines);
    }

    {
      List<String> lines = new ArrayList<>();

      lines.add("out.worker.count=1");
      lines.add("out.poll.duration.ms=800");

      appendLines(ownValues, lines);
    }
  }

  private static void appendLines(LinkedHashMap<String, String> values, List<String> lines) {
    for (String line : lines) {
      String[] split = line.split("=");
      if (split.length != 2) {
        throw new RuntimeException("Left value : " + line);
      }


      values.put(split[0], split[1]);
    }
  }
}
