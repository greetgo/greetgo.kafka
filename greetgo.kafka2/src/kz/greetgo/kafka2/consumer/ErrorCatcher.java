package kz.greetgo.kafka2.consumer;

public interface ErrorCatcher {
  void catchError(Throwable throwable);
}
