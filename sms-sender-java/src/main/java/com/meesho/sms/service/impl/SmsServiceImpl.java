package com.meesho.sms.service.impl;

import com.meesho.sms.dto.BlockResponse;
import com.meesho.sms.dto.SmsRequest;
import com.meesho.sms.dto.SmsResponse;
import com.meesho.sms.exception.BlockedUserException;
import com.meesho.sms.outbox.OutboxEvent;
import com.meesho.sms.outbox.OutboxRepository;
import com.meesho.sms.outbox.OutboxStatus;
import com.meesho.sms.service.SmsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SmsServiceImpl implements SmsService {

    private final StringRedisTemplate redisTemplate;
    private final OutboxRepository outboxRepository;

    @Override
    public SmsResponse sendSms(SmsRequest request) {
        log.info("Accepting SMS request for {}", request.getPhoneNumber());

        // Synchronous gate: reject blocked numbers immediately, before queueing work.
        Boolean isBlocked = redisTemplate.opsForSet().isMember("blocked_users", request.getPhoneNumber());
        if (Boolean.TRUE.equals(isBlocked)) {
            log.warn("Attempted to send SMS to blocked number: {}", request.getPhoneNumber());
            throw new BlockedUserException("Phone number is blocked from receiving SMS");
        }

        // Durably record the intent and return immediately. The vendor call and
        // Kafka publish happen later in OutboxProcessor — nothing on the request
        // path can be slowed or lost by a downstream outage.
        Instant now = Instant.now();
        OutboxEvent entry = OutboxEvent.builder()
                .id(UUID.randomUUID().toString())
                .phoneNumber(request.getPhoneNumber())
                .message(request.getMessage())
                .status(OutboxStatus.PENDING)
                .attempts(0)
                .nextAttemptAt(now)   // eligible for processing immediately
                .createdAt(now)
                .updatedAt(now)
                .build();
        outboxRepository.save(entry);

        log.info("SMS request {} persisted to outbox (PENDING)", entry.getId());

        return SmsResponse.builder()
                .status("ACCEPTED")
                .messageId(entry.getId())
                .message("SMS request accepted for processing")
                .build();
    }

    @Override
    public BlockResponse blockUser(String phoneNumber) {
        redisTemplate.opsForSet().add("blocked_users", phoneNumber);
        log.info("Blocked phone number: {}", phoneNumber);
        return BlockResponse.builder()
                .phoneNumber(phoneNumber)
                .blocked(true)
                .message("Phone number " + phoneNumber + " has been blocked")
                .build();
    }

    @Override
    public BlockResponse unblockUser(String phoneNumber) {
        redisTemplate.opsForSet().remove("blocked_users", phoneNumber);
        log.info("Unblocked phone number: {}", phoneNumber);
        return BlockResponse.builder()
                .phoneNumber(phoneNumber)
                .blocked(false)
                .message("Phone number " + phoneNumber + " has been unblocked")
                .build();
    }

    @Override
    public BlockResponse checkIfBlocked(String phoneNumber) {
        Boolean isBlocked = redisTemplate.opsForSet().isMember("blocked_users", phoneNumber);
        boolean blocked = Boolean.TRUE.equals(isBlocked);
        return BlockResponse.builder()
                .phoneNumber(phoneNumber)
                .blocked(blocked)
                .message(blocked ? "Phone number is blocked" : "Phone number is not blocked")
                .build();
    }
}
