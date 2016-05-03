package kz.greetgo.kafka.core;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class IdGeneratorTest {
  @Test
  public void newId() throws Exception {
    IdGenerator g = new IdGenerator();

    String id1 = g.newId();
    String id2 = g.newId();

    assertThat(id1).isNotNull();
    assertThat(id2).isNotNull();
    assertThat(id1).isNotEqualTo(id2);
  }
}