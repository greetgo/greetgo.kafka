package kz.greetgo.kafka.util;

import org.testng.annotations.Test;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import static kz.greetgo.kafka.util_for_tests.ReflectionUtil.findMethod;
import static org.fest.assertions.api.Assertions.assertThat;

@SuppressWarnings("InnerClassMayBeStatic")
public class AnnotationUtilTest {

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @interface Ann1 {
    String value();
  }

  @Ann1("7j6h84jvv9")
  class C1 {
    @SuppressWarnings("unused")
    @Ann1("5gh34v2gv7v")
    public void test() {}
  }

  class C1_Child extends C1 {
    @Override
    public void test() {}
  }

  @Test
  public void getAnnotation_fromMethod() {
    C1_Child object = new C1_Child();
    Method method = findMethod(object, "test");

    //
    //
    Ann1 annotation = AnnotationUtil.getAnnotation(method, Ann1.class);
    //
    //

    assertThat(annotation).isNotNull();
    assert annotation != null;
    assertThat(annotation.value()).isEqualTo("5gh34v2gv7v");
  }

  @Test
  public void getAnnotation_fromClass() {
    //
    //
    Ann1 annotation = AnnotationUtil.getAnnotation(C1_Child.class, Ann1.class);
    //
    //

    assertThat(annotation).isNotNull();
    assert annotation != null;
    assertThat(annotation.value()).isEqualTo("7j6h84jvv9");
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  @interface AnnPar1 {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  @interface AnnPar2 {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  @interface AnnPar3 {
    String value();
  }

  class C2 {

    @SuppressWarnings("unused")
    public void hello(@AnnPar1("x1") String x,
                      @AnnPar1("y1") String y,
                      @AnnPar1("z1") int z
    ) {}

  }

  class C2_Child extends C2 {
    @Override
    public void hello(@AnnPar2("x2") String x,
                      @AnnPar2("y2") String y,
                      @AnnPar2("z2") int z) {}
  }

  class C2_Child_Child extends C2_Child {
    @Override
    public void hello(@AnnPar3("x3") String x,
                      @AnnPar3("y3") String y,
                      @AnnPar3("z3") int z) {}
  }

  @Test
  public void getParameterAnnotations_C2() {
    C2_Child_Child object = new C2_Child_Child();

    Method method = findMethod(object, "hello");

    //
    //
    Annotation[][] annotations = AnnotationUtil.getParameterAnnotations(method);
    //
    //

    assertThat(annotations).hasSize(3);

    assertThat(annotations[0]).hasSize(3);
    assertThat(annotations[1]).hasSize(3);
    assertThat(annotations[2]).hasSize(3);

    assertThat(annotations[0][0]).isInstanceOf(AnnPar3.class);
    assertThat(annotations[1][0]).isInstanceOf(AnnPar3.class);
    assertThat(annotations[2][0]).isInstanceOf(AnnPar3.class);

    assertThat(annotations[0][1]).isInstanceOf(AnnPar2.class);
    assertThat(annotations[1][1]).isInstanceOf(AnnPar2.class);
    assertThat(annotations[2][1]).isInstanceOf(AnnPar2.class);

    assertThat(annotations[0][2]).isInstanceOf(AnnPar1.class);
    assertThat(annotations[1][2]).isInstanceOf(AnnPar1.class);
    assertThat(annotations[2][2]).isInstanceOf(AnnPar1.class);

  }

  class C3 {

    @SuppressWarnings("unused")
    public void hello(@AnnPar1("x1") String x,
                      @AnnPar1("y1") String y,
                      @AnnPar1("z1") int z
    ) {}

  }

  class C3_Child extends C3 {}

  class C3_Child_Child extends C3_Child {
    @Override
    public void hello(@AnnPar3("x3") String x,
                      @AnnPar3("y3") String y,
                      @AnnPar3("z3") int z) {}
  }

  @Test
  public void getParameterAnnotations_C3() {
    C3_Child_Child object = new C3_Child_Child();

    Method method = findMethod(object, "hello");

    //
    //
    Annotation[][] annotations = AnnotationUtil.getParameterAnnotations(method);
    //
    //

    assertThat(annotations).hasSize(3);

    assertThat(annotations[0]).hasSize(2);
    assertThat(annotations[1]).hasSize(2);
    assertThat(annotations[2]).hasSize(2);

    assertThat(annotations[0][0]).isInstanceOf(AnnPar3.class);
    assertThat(annotations[1][0]).isInstanceOf(AnnPar3.class);
    assertThat(annotations[2][0]).isInstanceOf(AnnPar3.class);

    assertThat(annotations[0][1]).isInstanceOf(AnnPar1.class);
    assertThat(annotations[1][1]).isInstanceOf(AnnPar1.class);
    assertThat(annotations[2][1]).isInstanceOf(AnnPar1.class);
  }


  class C4 {

    @SuppressWarnings("unused")
    public void hello(String x, @AnnPar1("11") @AnnPar2("12") String y, int z
    ) {}

  }

  class C4_Child extends C4 {
    @Override
    public void hello(String x, String y, int z) {}
  }

  class C4_Child_Child extends C4_Child {
    @Override
    public void hello(String x, @AnnPar3("y3") String y, int z) {}
  }

  @Test
  public void getParameterAnnotations_C4() {
    C4_Child_Child object = new C4_Child_Child();

    Method method = findMethod(object, "hello");

    //
    //
    Annotation[][] annotations = AnnotationUtil.getParameterAnnotations(method);
    //
    //

    assertThat(annotations).hasSize(3);

    assertThat(annotations[0]).hasSize(0);
    assertThat(annotations[1]).hasSize(3);
    assertThat(annotations[2]).hasSize(0);

    assertThat(annotations[1][0]).isInstanceOf(AnnPar3.class);
    assertThat(annotations[1][1]).isInstanceOf(AnnPar1.class);
    assertThat(annotations[1][2]).isInstanceOf(AnnPar2.class);
  }

}
