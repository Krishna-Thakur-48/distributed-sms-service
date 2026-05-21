package com.meesho.sms.event;

public interface SmsEventPublisher {
    void publish(SmsEvent event);
}
