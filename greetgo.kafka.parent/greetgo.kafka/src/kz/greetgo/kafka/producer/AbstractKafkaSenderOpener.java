package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.HasId;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.kafka.str.StrConverterXml;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public abstract class AbstractKafkaSenderOpener implements KafkaSenderOpener {

  protected abstract String getBootstrapServers();

  private StrConverter strConverter = null;

  protected StrConverter strConverter() {
    if (strConverter == null) {
      strConverter = createStrConverter();
      prepareStrConverter(strConverter);
    }

    return strConverter;
  }

  protected StrConverter createStrConverter() {
    StrConverterXml strConverter = new StrConverterXml();
    strConverter.useClass(Box.class, Box.class.getSimpleName());
    prepareStrConverter(strConverter);
    return strConverter;
  }

  protected abstract void prepareStrConverter(StrConverter strConverter);

  protected abstract String author();

  protected abstract String topic();

  protected String extractId(Object object) {
    if (object instanceof HasId) return ((HasId) object).getId();
    return extractAlternativeId(object);
  }

  protected String extractAlternativeId(Object object) {
    throw new IllegalArgumentException("Cannot extract id from " + object.getClass());
  }

  protected Properties createProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", getBootstrapServers());
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());
    return props;
  }

  private Properties properties = null;

  private Properties getProperties() {
    return properties == null ? properties = createProperties() : properties;
  }

  @Override
  public KafkaSender open() {


    return new KafkaSender() {
      KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());

      @Override
      public void send(Object object) {
        if (object == null) throw new NullPointerException();
        if (producer == null) throw new RuntimeException("Sender already closed");

        Box box = new Box();
        box.a = author();
        box.n = System.nanoTime();
        box.t = new Date();
        box.body = object;

        String value = strConverter().toStr(box);
        String key = extractId(object);

        try {
          producer.send(new ProducerRecord<String, String>(topic(), key, value)).get();
        } catch (InterruptedException e) {
          throw new RuntimeInterruptedException(e);
        } catch (ExecutionException e) {
          throw new RuntimeExecutionException(e);
        }
      }

      @Override
      public void close() throws Exception {
        producer.close();
        producer = null;
      }
    };
  }
}
