package kz.greetgo.kafka.core;

import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.producer.KafkaFuture;

import java.util.ArrayList;
import java.util.List;

public class ProducerSynchronizer {
  private final Logger logger;

  public ProducerSynchronizer(Logger logger) {
    this.logger = logger;
  }


  private class ConsumerContext {
    void close() {
      try {
        for (KafkaFuture kafkaFuture : futureList) {
          kafkaFuture.awaitAndGet();
        }
      } catch (Exception exception) {
        logger.logProducerAwaitAndGetError("JO2wqXe3gh", exception);
      }
    }

    private final List<KafkaFuture> futureList = new ArrayList<>();

    public void append(KafkaFuture kafkaFuture) {
      futureList.add(kafkaFuture);
    }
  }

  private final ThreadLocal<ConsumerContext> consumerContextHolder = InheritableThreadLocal.withInitial(() -> null);

  public ConsumerPortionInvoking startConsumerPortionInvoking() {
    ConsumerContext consumerContext = new ConsumerContext();
    consumerContextHolder.set(consumerContext);
    return () -> {
      consumerContext.close();
      consumerContextHolder.set(null);
    };
  }

  public void acceptKafkaFuture(KafkaFuture kafkaFuture) {
    ConsumerContext consumerContext = consumerContextHolder.get();
    if (consumerContext == null) {
      kafkaFuture.awaitAndGet();
      return;
    }

    consumerContext.append(kafkaFuture);

  }
}
