package com.meesho.sms.exception;

/**
 * Thrown by the Kafka publisher when delivery cannot be confirmed. Caught only
 * by the outbox worker, which leaves the row in SENT and retries next cycle.
 * It never reaches a request thread, so there is no HTTP handler for it.
 */
public class KafkaPublishException extends RuntimeException {
    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
