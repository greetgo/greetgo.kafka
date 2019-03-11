package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.consumer.annotations.Topic;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ConsumerDefinitionExtractor {

  public ConsumerLogger consumerLogger;
  public String hostId;

  public List<ConsumerDefinition> extract(Object controller) {

    List<ConsumerDefinition> ret = new ArrayList<>();

    for (Method method : controller.getClass().getMethods()) {

      Topic topic = method.getAnnotation(Topic.class);
      if (topic == null) {
        continue;
      }

      {
        ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, consumerLogger, hostId);

        ret.add(consumerDefinition);
      }
    }

    return ret;
  }

}
