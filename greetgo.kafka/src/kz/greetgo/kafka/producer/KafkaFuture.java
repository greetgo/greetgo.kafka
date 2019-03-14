package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.errors.future.ExecutionExceptionWrapper;
import kz.greetgo.kafka.errors.future.InterruptedExceptionWrapper;
import kz.greetgo.kafka.errors.future.TimeoutExceptionWrapper;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaFuture {

  private final Future<RecordMetadata> source;

  public KafkaFuture(Future<RecordMetadata> source) {
    this.source = source;
  }

  public boolean cancel(boolean mayInterruptIfRunning) {
    return source.cancel(mayInterruptIfRunning);
  }

  public boolean isCancelled() {
    return source.isCancelled();
  }

  public boolean isDone() {
    return source.isDone();
  }

  public RecordMetadata awaitAndGet() {
    try {
      return source.get();
    } catch (InterruptedException e) {
      throw new InterruptedExceptionWrapper(e);
    } catch (ExecutionException e) {
      throw new ExecutionExceptionWrapper(e);
    }
  }

  public RecordMetadata get(long timeout, TimeUnit unit) {
    try {
      return source.get(timeout, unit);
    } catch (InterruptedException e) {
      throw new InterruptedExceptionWrapper(e);
    } catch (ExecutionException e) {
      throw new ExecutionExceptionWrapper(e);
    } catch (TimeoutException e) {
      throw new TimeoutExceptionWrapper(e);
    }
  }
}
