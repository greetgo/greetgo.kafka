package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.consumer.annotations.Topic;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.fest.assertions.api.Assertions.assertThat;

@SuppressWarnings("InnerClassMayBeStatic")
public class ConsumerDefinitionExtractorTest {

  class ControllerClass {

    @Topic("topic1")
    public void method1(Object object) {}

    @Topic({"topic2", "topic3"})
    public void method2(Object object) {}

  }

  @Test
  public void extract__direct() {
    Object controller = new ControllerClass();

    ConsumerDefinitionExtractor extractor = new ConsumerDefinitionExtractor();

    List<ConsumerDefinition> list = extractor.extract(controller);

    Map<String, ConsumerDefinition> map = list
      .stream()
      .collect(Collectors.toMap(ConsumerDefinition::getConsumerName, x -> x));

    assertThat(map).containsKey("method1");
    assertThat(map).containsKey("method2");
  }

  @Test
  public void extract__throughInlineChildClass() {
    Object controller = new ControllerClass() {};

    ConsumerDefinitionExtractor extractor = new ConsumerDefinitionExtractor();

    List<ConsumerDefinition> list = extractor.extract(controller);

    Map<String, ConsumerDefinition> map = list
      .stream()
      .collect(Collectors.toMap(ConsumerDefinition::getConsumerName, x -> x));

    assertThat(map).containsKey("method1");
    assertThat(map).containsKey("method2");
  }

  @Test
  public void extract__throughOverrideInlineChildClass() {
    Object controller = new ControllerClass() {
      @Override
      public void method1(Object object) {
        super.method1(object);
      }
    };

    ConsumerDefinitionExtractor extractor = new ConsumerDefinitionExtractor();

    List<ConsumerDefinition> list = extractor.extract(controller);

    Map<String, ConsumerDefinition> map = list
      .stream()
      .collect(Collectors.toMap(ConsumerDefinition::getConsumerName, x -> x));

    assertThat(map).containsKey("method1");
    assertThat(map).containsKey("method2");
  }

  class ChildControllerClass extends ControllerClass {

    @Override
    public void method1(Object object) {}

  }

  @Test
  public void extract__throughOverrideChildClass() {
    Object controller = new ChildControllerClass();

    ConsumerDefinitionExtractor extractor = new ConsumerDefinitionExtractor();

    List<ConsumerDefinition> list = extractor.extract(controller);

    Map<String, ConsumerDefinition> map = list
      .stream()
      .collect(Collectors.toMap(ConsumerDefinition::getConsumerName, x -> x));

    assertThat(map).containsKey("method1");
    assertThat(map).containsKey("method2");
  }

  class ChildChildControllerClass extends ChildControllerClass {

    @Override
    public void method1(Object object) {}

  }

  @Test
  public void extract__throughOverrideChildChildClass() {
    Object controller = new ChildChildControllerClass();

    ConsumerDefinitionExtractor extractor = new ConsumerDefinitionExtractor();

    List<ConsumerDefinition> list = extractor.extract(controller);

    Map<String, ConsumerDefinition> map = list
      .stream()
      .collect(Collectors.toMap(ConsumerDefinition::getConsumerName, x -> x));

    assertThat(map).containsKey("method1");
    assertThat(map).containsKey("method2");
  }
}
