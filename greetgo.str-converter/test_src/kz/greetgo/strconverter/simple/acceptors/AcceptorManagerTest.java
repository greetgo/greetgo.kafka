package kz.greetgo.strconverter.simple.acceptors;

import kz.greetgo.util.RND;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class AcceptorManagerTest {

  public static class FieldTestClass {
    public String wow;
  }

  @Test
  public void setField() {

    AcceptorManager acceptorManager = new AcceptorManager(FieldTestClass.class);

    AttrAcceptor acceptorWow = acceptorManager.acceptor("wow");

    FieldTestClass test = new FieldTestClass();

    String value = RND.str(10);

    test.wow = RND.str(10);

    assertThat(test.wow).isNotEqualTo(value);

    //
    //
    acceptorWow.set(test, value);
    //
    //

    assertThat(test.wow).isEqualTo(value);

  }

  @Test
  public void getField() {

    AcceptorManager acceptorManager = new AcceptorManager(FieldTestClass.class);

    AttrAcceptor acceptorWow = acceptorManager.acceptor("wow");

    FieldTestClass test = new FieldTestClass();
    test.wow = RND.str(10);

    //
    //
    Object actual = acceptorWow.get(test);
    //
    //

    assertThat(actual).isEqualTo(test.wow);
  }

  public static class MethodTestClass {
    public String field;

    @SuppressWarnings("unused")
    public String getWow() {
      return field;
    }

    @SuppressWarnings("unused")
    public void setWow(String wow) {
      this.field = wow;
    }
  }

  @Test
  public void setMethod() {

    AcceptorManager acceptorManager = new AcceptorManager(MethodTestClass.class);

    AttrAcceptor acceptorWow = acceptorManager.acceptor("wow");

    MethodTestClass test = new MethodTestClass();
    test.field = RND.str(10);

    String value = RND.str(10);

    assertThat(test.field).isNotEqualTo(value);

    //
    //
    acceptorWow.set(test, value);
    //
    //

    assertThat(test.field).isEqualTo(value);

  }

  @Test
  public void getMethod() {

    AcceptorManager acceptorManager = new AcceptorManager(MethodTestClass.class);

    AttrAcceptor acceptorWow = acceptorManager.acceptor("wow");

    MethodTestClass test = new MethodTestClass();
    test.field = RND.str(10);

    //
    //
    Object actual = acceptorWow.get(test);
    //
    //

    assertThat(actual).isEqualTo(test.field);
  }

  public static class LeftSetMethodName {

    @SuppressWarnings("unused")
    public void set(String x) {}

  }

  @Test
  public void testLeftSetMethodName() {
    AcceptorManager acceptorManager = new AcceptorManager(LeftSetMethodName.class);

    AttrAcceptor acceptor = acceptorManager.acceptor("");
    assertThat(acceptor).isNull();

  }

  public static class LeftGetMethodName {

    @SuppressWarnings("unused")
    public String get() {
      return null;
    }

  }

  @Test
  public void testLeftGetMethodName() {

    AcceptorManager acceptorManager = new AcceptorManager(LeftGetMethodName.class);

    AttrAcceptor acceptor = acceptorManager.acceptor("");
    assertThat(acceptor).isNull();

  }

}
