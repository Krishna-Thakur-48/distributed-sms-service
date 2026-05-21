package com.meesho.sms.service.impl;

import com.meesho.sms.dto.SmsRequest;
import com.meesho.sms.dto.SmsResponse;
import com.meesho.sms.event.SmsEvent;
import com.meesho.sms.event.SmsEventPublisher;
import com.meesho.sms.service.SmsService;
import com.meesho.sms.exception.BlockedUserException;
import com.meesho.sms.vendor.SmsVendorService;
import com.meesho.sms.vendor.VendorResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class SmsServiceImpl implements SmsService {

    private final StringRedisTemplate redisTemplate;
    private final SmsVendorService vendorService;
    private final SmsEventPublisher eventPublisher;

    @Override
    public SmsResponse sendSms(SmsRequest request) {
        log.info("Sending SMS to {}", request.getPhoneNumber());

        Boolean isBlocked = redisTemplate.opsForSet().isMember("blocked_users", request.getPhoneNumber());
        if (Boolean.TRUE.equals(isBlocked)) {
            log.warn("Attempted to send SMS to blocked number: {}", request.getPhoneNumber());
            throw new BlockedUserException("Phone number is blocked from receiving SMS");
        }
        
        VendorResponse vendorResponse = vendorService.send(request.getPhoneNumber(), request.getMessage());
        
        // Build the Event
        SmsEvent event = SmsEvent.builder()
                .phoneNumber(request.getPhoneNumber())
                .message(request.getMessage())
                .status(vendorResponse.isSuccess() ? "SUCCESS" : "FAILED")
                .vendorMessageId(vendorResponse.getVendorMessageId())
                .errorReason(vendorResponse.getErrorReason())
                .timestamp(OffsetDateTime.now())
                .build();
                
        // Publish Event
        eventPublisher.publish(event);
        
        if (vendorResponse.isSuccess()) {
            return SmsResponse.builder()
                    .status("SUCCESS")
                    .messageId(vendorResponse.getVendorMessageId())
                    .message("SMS sent successfully to " + request.getPhoneNumber())
                    .build();
        } else {
            return SmsResponse.builder()
                    .status("FAILED")
                    .messageId(null)
                    .message("SMS failed to send: " + vendorResponse.getErrorReason())
                    .build();
        }
    }
}
