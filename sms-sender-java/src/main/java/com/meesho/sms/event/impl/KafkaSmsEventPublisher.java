package com.meesho.sms.event.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.sms.config.KafkaConfig;
import com.meesho.sms.event.SmsEvent;
import com.meesho.sms.event.SmsEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaSmsEventPublisher implements SmsEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void publish(SmsEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            log.info("Publishing SMS event to Kafka: {}", payload);
            kafkaTemplate.send(KafkaConfig.SMS_EVENTS_TOPIC, event.getPhoneNumber(), payload);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize SMS event to JSON", e);
        }
    }
}
