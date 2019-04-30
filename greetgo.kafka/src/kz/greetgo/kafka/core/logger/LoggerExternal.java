package kz.greetgo.kafka.core.logger;

public interface LoggerExternal {

  void setDestination(LoggerDestination destination);

  void setDestination(LogMessageAcceptor acceptor);

  void setShowLogger(LoggerType loggerType, boolean show);

}
