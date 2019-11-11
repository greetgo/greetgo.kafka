package kz.greetgo.kafka.util;

import kz.greetgo.kafka.core.PushFilter;

public class PushFilterOnControllerClasses {

  public static PushFilter on(Class<?>... controllerClass) {
    return consumerDefinition -> {
      Object controller = consumerDefinition.getController();
      for (Class<?> aClass : controllerClass) {
        if (aClass.isInstance(controller)) {
          return true;
        }
      }
      return false;
    };
  }

}
