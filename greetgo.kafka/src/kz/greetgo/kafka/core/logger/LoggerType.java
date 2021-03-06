package kz.greetgo.kafka.core.logger;

public enum LoggerType {
  SHOW_PRODUCER_CONFIG,
  SHOW_CONSUMER_WORKER_CONFIG,
  LOG_CLOSE_PRODUCER,
  LOG_START_CONSUMER_WORKER,
  LOG_CONSUMER_POLL_EXCEPTION_HAPPENED,
  LOG_CONSUMER_COMMIT_SYNC_EXCEPTION_HAPPENED,
  LOG_CONSUMER_FINISH_WORKER,
  LOG_CONSUMER_ILLEGAL_ACCESS_EXCEPTION_INVOKING_METHOD,
  LOG_CONSUMER_ERROR_IN_METHOD,
  LOG_CONSUMER_REACTOR_REFRESH,
  LOG_CREATE_PRODUCER,
  LOG_PRODUCER_VALIDATION_ERROR,
}
