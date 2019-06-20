package kz.greetgo.kafka.core.logger;

public interface LogMessageAcceptor {

  boolean isInfoEnabled();

  void info(String message);

  boolean isDebugEnabled();

  void debug(String message);

  void error(String message);

}
