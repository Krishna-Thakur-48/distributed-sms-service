package com.meesho.sms.event.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.sms.config.KafkaConfig;
import com.meesho.sms.event.SmsEvent;
import com.meesho.sms.event.SmsEventPublisher;
import com.meesho.sms.exception.KafkaPublishException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaSmsEventPublisher implements SmsEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Publishes synchronously and waits for the broker acknowledgment. This is
     * called only from the outbox worker thread (never the request thread), so
     * blocking here is intentional: the worker must know delivery succeeded
     * before it marks the row PUBLISHED. On failure it throws and the worker
     * retries the row next cycle.
     */
    @Override
    public void publish(SmsEvent event) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new KafkaPublishException("Failed to serialize SMS event", e);
        }

        try {
            log.info("Publishing SMS event to Kafka: {}", payload);
            kafkaTemplate.send(KafkaConfig.SMS_EVENTS_TOPIC, event.getPhoneNumber(), payload)
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while publishing to Kafka", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException("Failed to publish SMS event to Kafka", e);
        }
    }
}
