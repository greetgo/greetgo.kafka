package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.core.logger.Logger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ConsumerDefinitionExtractor {

  public Logger logger;
  public String hostId;

  public List<ConsumerDefinition> extract(Object controller) {

    List<ConsumerDefinition> ret = new ArrayList<>();

    for (Method method : controller.getClass().getMethods()) {

      Topic topic = method.getAnnotation(Topic.class);
      if (topic == null) {
        continue;
      }

      {
        ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, logger, hostId);

        ret.add(consumerDefinition);
      }
    }

    return ret;
  }

}
