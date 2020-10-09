package kz.greetgo.kafka.util_for_tests;

import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.KafkaFuture;
import kz.greetgo.kafka.producer.KafkaPortionSending;
import kz.greetgo.kafka.producer.KafkaSending;
import kz.greetgo.kafka.producer.ProducerFacade;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestProducerFacade implements ProducerFacade {

  public int resetCount = 0;

  public int indexer = 1;
  public int lastResetIndex;

  public static class Sent {
    public final Object model;

    public String topic = null;
    public Integer partition = null;
    public Long timestamp = null;
    public final List<String> ignoredConsumerNames = new ArrayList<>();
    public String author;
    public final ArrayList<Header> headers = new ArrayList<>();

    public int sendingIndex = 0;
    public int goIndex = 0;
    public int awaitAndGetIndex = 0;
    public byte[] keyAsBytes;

    public Sent(Object model) {
      this.model = model;
    }

    public void validate() {
      try {
        Box.validateBody(model);
      } catch (Throwable throwable) {
        if (throwable instanceof RuntimeException) {
          throw (RuntimeException) throwable;
        }
        throw new RuntimeException(throwable);
      }
    }
  }

  public final List<Sent> sentList = new ArrayList<>();

  @Override
  public void reset() {
    resetCount++;
    lastResetIndex = indexer++;
  }

  @Override
  public Producer<byte[], Box> getNativeProducer() {
    throw new RuntimeException("Not working");
  }

  @Override
  public Map<String, Object> getConfigData() {
    throw new RuntimeException("Not working");
  }

  @Override
  public KafkaSending sending(Object body) {
    final Sent sent = new Sent(body);
    sent.sendingIndex = indexer++;
    return new KafkaSending() {
      @Override
      public KafkaSending toTopic(String topic) {
        sent.topic = topic;
        return this;
      }

      @Override
      public KafkaSending toPartition(int partition) {
        sent.partition = partition;
        return this;
      }

      @Override
      public KafkaSending setTimestamp(Long timestamp) {
        sent.timestamp = timestamp;
        return this;
      }

      @Override
      public KafkaSending addConsumerToIgnore(String consumerName) {
        sent.ignoredConsumerNames.add(consumerName);
        return this;
      }

      @Override
      public KafkaSending setAuthor(String author) {
        sent.author = author;
        return this;
      }

      @Override
      public KafkaSending addHeader(String key, byte[] value) {
        sent.headers.add(new Header() {
          @Override
          public String key() {
            return key;
          }

          @Override
          public byte[] value() {
            return value;
          }
        });
        return this;
      }

      @Override
      public KafkaSending withKey(String keyAsString) {
        sent.keyAsBytes = keyAsString.getBytes(StandardCharsets.UTF_8);
        return this;
      }

      @Override
      public KafkaSending withKey(byte[] keyAsBytes) {
        sent.keyAsBytes = keyAsBytes;
        return this;
      }

      @Override
      public KafkaFuture go() {
        sent.goIndex = indexer++;
        sentList.add(sent);
        sent.validate();
        return new KafkaFuture(new Future<RecordMetadata>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            throw new RuntimeException("Not working");
          }

          @Override
          public boolean isCancelled() {
            throw new RuntimeException("Not working");
          }

          @Override
          public boolean isDone() {
            throw new RuntimeException("Not working");
          }

          @Override
          public RecordMetadata get() {
            sent.awaitAndGetIndex = indexer++;
            return null;
          }

          @Override
          public RecordMetadata get(long timeout, @SuppressWarnings("NullableProblems") TimeUnit unit) {
            throw new RuntimeException("Not working");
          }
        });
      }
    };
  }

  @Override
  public KafkaPortionSending portionSending(Object body) {
    final Sent sent = new Sent(body);
    sent.sendingIndex = indexer++;
    return new KafkaPortionSending() {
      @Override
      public KafkaPortionSending toTopic(String topic) {
        sent.topic = topic;
        return this;
      }

      @Override
      public KafkaPortionSending toPartition(int partition) {
        sent.partition = partition;
        return this;
      }

      @Override
      public KafkaPortionSending setTimestamp(Long timestamp) {
        sent.timestamp = timestamp;
        return this;
      }

      @Override
      public KafkaPortionSending addConsumerToIgnore(String consumerName) {
        sent.ignoredConsumerNames.add(consumerName);
        return this;
      }

      @Override
      public KafkaPortionSending setAuthor(String author) {
        sent.author = author;
        return this;
      }

      @Override
      public KafkaPortionSending addHeader(String key, byte[] value) {
        sent.headers.add(new Header() {
          @Override
          public String key() {
            return key;
          }

          @Override
          public byte[] value() {
            return value;
          }
        });
        return this;
      }

      @Override
      public KafkaPortionSending withKey(String keyAsString) {
        sent.keyAsBytes = keyAsString.getBytes(StandardCharsets.UTF_8);
        return this;
      }

      @Override
      public KafkaPortionSending withKey(byte[] keyAsBytes) {
        sent.keyAsBytes = keyAsBytes;
        return this;
      }

      @Override
      public void go() {
        sent.goIndex = indexer++;
        sentList.add(sent);
        sent.validate();
        sent.awaitAndGetIndex = indexer++;
      }
    };
  }
}
