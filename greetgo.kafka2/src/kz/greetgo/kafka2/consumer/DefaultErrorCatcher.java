package kz.greetgo.kafka2.consumer;

public class DefaultErrorCatcher implements ErrorCatcher {
  @Override
  public void catchError(Throwable throwable) {
    throwable.printStackTrace();
  }
}
