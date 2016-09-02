package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.HasId;
import kz.greetgo.kafka.core.Head;
import kz.greetgo.kafka.str.StrConverter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public abstract class AbstractKafkaSender implements KafkaSender {

  protected abstract String getBootstrapServers();

  private StrConverter strConverter = null;

  public StrConverter strConverter() {
    return strConverter == null ? strConverter = createStrConverter() : strConverter;
  }

  protected abstract StrConverter createStrConverter();

  protected abstract String author();

  protected abstract Set<String> ignorableConsumers(String author, Object sendingObject, String key);

  protected abstract String topic();

  protected String extractId(Object object) {
    if (object == null) throw new NullPointerException("Cannot extract id from null");

    if (object instanceof HasId) return ((HasId) object).getId();

    if (object instanceof List) {
      return extractIdFromList((List) object);
    }

    return alternativelyExtractId(object);
  }

  private String extractIdFromList(List objectList) {
    if (objectList.size() == 0) {
      throw new RuntimeException("List cannot be empty");
    }

    String id = extractId(objectList.get(0));

    for (int i = 1, size = objectList.size(); i < size; i++) {
      String ithId = extractId(objectList.get(i));
      if (!id.equals(ithId)) {
        throw new RuntimeException("Ids in list must be same, but 0-th id = " + id + ", " + i + "-th id = " + ithId);
      }
    }

    return id;
  }

  protected String alternativelyExtractId(Object object) {
    throw new IllegalArgumentException("Cannot extract id from " + object.getClass());
  }

  protected Properties createProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", getBootstrapServers());
    props.put("acks", "all");
    props.put("retries", 10_000);
    props.put("max.in.flight.requests.per.connection", 1);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());
    return props;
  }

  private Properties properties = null;

  private Properties getProperties() {
    return properties == null ? properties = createProperties() : properties;
  }

  @Override
  public KafkaSending open() {
    return new KafkaSending() {
      KafkaProducer<String, String> producer = getProducer();

      @Override
      public void send(Object object) {
        if (object == null) throw new NullPointerException();
        if (producer == null) throw new RuntimeException("Sender already closed");

        final String author = author();
        String key = extractId(object);

        Box box = new Box();
        box.head = new Head();
        box.head.a = author;
        box.head.n = System.nanoTime();
        box.head.t = new Date();
        box.head.ign = ignorableConsumers(author, object, key);
        box.body = object;

        String value = strConverter().toStr(box);

        try {
          producer.send(new ProducerRecord<>(topic(), key, value)).get();
        } catch (InterruptedException e) {
          throw new RuntimeInterruptedException(e);
        } catch (ExecutionException e) {
          throw new RuntimeExecutionException(e);
        }
      }

      @Override
      public void close() {
        producer.flush();
        producer = null;
      }
    };
  }

  private KafkaProducer<String, String> producer = null;

  private KafkaProducer<String, String> getProducer() {
    if (producer != null) return producer;
    synchronized (this) {
      if (producer != null) return producer;
      return producer = new KafkaProducer<>(getProperties());
    }
  }

  public void close() {
    if (producer == null) return;

    KafkaProducer<String, String> localProducer;

    synchronized (this) {
      if (producer == null) return;
      localProducer = producer;
      producer = null;
    }

    localProducer.close();
  }

}
