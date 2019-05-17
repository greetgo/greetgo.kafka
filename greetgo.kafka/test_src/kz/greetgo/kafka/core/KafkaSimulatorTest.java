package kz.greetgo.kafka.core;

import kz.greetgo.kafka.ModelKryo;
import kz.greetgo.kafka.ModelKryo2;
import kz.greetgo.kafka.consumer.annotations.ConsumersFolder;
import kz.greetgo.kafka.consumer.annotations.GroupId;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.strconverter.simple.StrConverterSimple;
import kz.greetgo.util.RND;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class KafkaSimulatorTest {


  @ConsumersFolder("top")
  public static class TestConsumer {

    public final List<ModelKryo> modelList = new ArrayList<>();

    @Topic("test_topic1")
    @GroupId("gr1")
    public void consumer1(ModelKryo model) {
      modelList.add(model);
    }

    public final List<ModelKryo2> model2List = new ArrayList<>();

    @Topic("test_topic2")
    @GroupId("gr2")
    public void consumer2(ModelKryo2 model) {
      model2List.add(model);
    }

  }

  @Test
  public void testSimulator() {

    TestConsumer controller = new TestConsumer();

    KafkaSimulator simulator = new KafkaSimulator();

    simulator.authorGetter = () -> "asd";
    simulator.hostId = "asd";

    simulator.addController(controller);

    StrConverterSimple strConverter = new StrConverterSimple();
    strConverter.convertRegistry().register(Box.class);
    strConverter.convertRegistry().register(ModelKryo.class);
    strConverter.convertRegistry().register(ModelKryo2.class);

    simulator.setStrConverterSupplier(() -> strConverter);

    simulator.startConsumers();

    ProducerFacade producer = simulator.createProducer("test");

    ModelKryo object1 = new ModelKryo();
    object1.name = RND.str(10);
    object1.wow = 234L;
    producer.sending(object1).toTopic("test_topic1").go().awaitAndGet();

    ModelKryo2 object2 = new ModelKryo2();
    object2.surname = RND.str(10);
    object2.id = 234L;
    producer.sending(object2).toTopic("test_topic2").go().awaitAndGet();

    assertThat(controller.modelList).isEmpty();
    assertThat(controller.model2List).isEmpty();

    simulator.push();

    assertThat(controller.modelList).hasSize(1);
    assertThat(controller.model2List).hasSize(1);
  }

}
