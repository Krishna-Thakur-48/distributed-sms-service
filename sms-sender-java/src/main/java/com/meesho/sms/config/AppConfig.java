package com.meesho.sms.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class AppConfig {
    // @EnableScheduling activates the @Scheduled outbox worker (OutboxProcessor).
}
