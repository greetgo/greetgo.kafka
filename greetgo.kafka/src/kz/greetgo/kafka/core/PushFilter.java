package kz.greetgo.kafka.core;

import kz.greetgo.kafka.consumer.ConsumerDefinition;

public interface PushFilter {

  boolean canPush(ConsumerDefinition consumerDefinition);

}
