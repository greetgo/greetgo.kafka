package kz.greetgo.kafka.core.logger;

public interface LogMessageAcceptor {

  void info(String message);

  void error(String message);

}
