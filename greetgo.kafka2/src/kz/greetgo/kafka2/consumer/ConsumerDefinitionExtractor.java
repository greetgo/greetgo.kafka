package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.consumer.ConsumerDefinition;
import kz.greetgo.kafka2.consumer.Topic;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ConsumerDefinitionExtractor {

  public static List<ConsumerDefinition> extract(Object controller) {

    List<ConsumerDefinition> ret = new ArrayList<>();

    for (Method method : controller.getClass().getMethods()) {

      Topic topic = method.getAnnotation(Topic.class);
      if (topic == null) {
        continue;
      }

      ret.add(new ConsumerDefinition(controller, method));
    }

    return ret;
  }

}
