package kz.greetgo.kafka_old.core;


import kz.greetgo.strconverter.StrConverter;

public class StrConverterPreparationBased {
  public static void prepare(StrConverter strConverter) {
    strConverter.useClass(Box.class, Box.class.getSimpleName());
    strConverter.useClass(Head.class, Head.class.getSimpleName());
  }
}
