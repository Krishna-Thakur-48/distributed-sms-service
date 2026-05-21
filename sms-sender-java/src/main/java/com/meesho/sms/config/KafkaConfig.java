package com.meesho.sms.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    public static final String SMS_EVENTS_TOPIC = "sms-events-topic";

    @Bean
    public NewTopic smsEventsTopic() {
        return TopicBuilder.name(SMS_EVENTS_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
