package kz.greetgo.kafka.massive.tests;

import kz.greetgo.kafka.core.logger.LogMessageAcceptor;

public class SimplePrinter implements LogMessageAcceptor {
  @Override
  public boolean isInfoEnabled() {
    return true;
  }

  @Override
  public void info(String message) {
    System.out.println("INFO " + message);
  }

  @Override
  public boolean isDebugEnabled() {
    return true;
  }

  @Override
  public void debug(String message) {
    System.out.println("DEBUG " + message);
  }

  @Override
  public void error(String message, Throwable throwable) {
    System.out.println("ERROR " + message);
    throwable.printStackTrace();
  }
}
