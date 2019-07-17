package kz.greetgo.kafka.massive.tests;

import kz.greetgo.class_scanner.ClassScanner;
import kz.greetgo.class_scanner.ClassScannerDef;
import kz.greetgo.kafka.core.KafkaReactor;
import kz.greetgo.kafka.massive.tests.model.KafkaModel;
import kz.greetgo.strconverter.simple.StrConverterSimple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ModelRegistrar {

  private ModelRegistrar() {}

  public static StrConverterSimple createStrConverterSimple() {
    final List<Class> kafkaModelList = new ArrayList<>();

    ClassScanner classScanner = new ClassScannerDef();

    Set<Class<?>> classes = classScanner.scanPackage(KafkaModel.class.getPackage().getName());
    for (Class<?> aClass : classes) {
      if (aClass.getAnnotation(KafkaModel.class) != null) {

        kafkaModelList.add(aClass);

      }
    }

    StrConverterSimple strConverter = new StrConverterSimple();
    strConverter.convertRegistry().register(kz.greetgo.kafka.model.Box.class);
    for (Class kafkaModel : kafkaModelList) {
      strConverter.convertRegistry().register(kafkaModel);
    }

    return strConverter;
  }

  public static void registrar(KafkaReactor reactor) {

    StrConverterSimple strConverter = createStrConverterSimple();

    reactor.setStrConverterSupplier(() -> strConverter);

  }

}
