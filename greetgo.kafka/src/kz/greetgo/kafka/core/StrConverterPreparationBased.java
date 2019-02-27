package kz.greetgo.kafka.core;

import kz.greetgo.kafka.str.StrConverter;

public class StrConverterPreparationBased {
  public static void prepare(StrConverter strConverter) {
    strConverter.useClass(Box.class, Box.class.getSimpleName());
    strConverter.useClass(Head.class, Head.class.getSimpleName());
  }
}
