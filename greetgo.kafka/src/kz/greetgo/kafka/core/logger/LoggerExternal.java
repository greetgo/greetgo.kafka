package kz.greetgo.kafka.core.logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Predicate;

public interface LoggerExternal {

  void setDestination(LoggerDestination destination);

  void setDestination(LogMessageAcceptor acceptor);

  void setLoggerTypeFilter(Predicate<LoggerType> loggerTypeFilter);

  default void setShowLoggerTypes(Collection<LoggerType> loggerTypes) {
    HashSet<LoggerType> local = new HashSet<>(loggerTypes);
    setLoggerTypeFilter(local::contains);
  }

}
