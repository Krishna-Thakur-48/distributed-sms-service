package com.meesho.sms.service;

import com.meesho.sms.dto.SmsRequest;
import com.meesho.sms.dto.SmsResponse;
import com.meesho.sms.exception.BlockedUserException;
import com.meesho.sms.outbox.OutboxEvent;
import com.meesho.sms.outbox.OutboxRepository;
import com.meesho.sms.outbox.OutboxStatus;
import com.meesho.sms.service.impl.SmsServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SmsServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private SetOperations<String, String> setOperations;

    @Mock
    private OutboxRepository outboxRepository;

    @Captor
    private ArgumentCaptor<OutboxEvent> outboxCaptor;

    private SmsService smsService;

    @BeforeEach
    void setUp() {
        smsService = new SmsServiceImpl(redisTemplate, outboxRepository);
    }

    @Test
    void sendSms_WhenNotBlocked_AcceptsAndWritesPendingOutboxRow() {
        SmsRequest request = new SmsRequest("+1234567890", "Test message");
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(setOperations.isMember("blocked_users", "+1234567890")).thenReturn(false);

        SmsResponse response = smsService.sendSms(request);

        // The request is accepted, not yet delivered.
        assertNotNull(response);
        assertEquals("ACCEPTED", response.getStatus());
        assertNotNull(response.getMessageId()); // the outbox row id

        // A PENDING row is persisted, carrying the request details.
        verify(outboxRepository).save(outboxCaptor.capture());
        OutboxEvent saved = outboxCaptor.getValue();
        assertEquals("+1234567890", saved.getPhoneNumber());
        assertEquals("Test message", saved.getMessage());
        assertEquals(OutboxStatus.PENDING, saved.getStatus());
        assertEquals(response.getMessageId(), saved.getId());
        assertNotNull(saved.getCreatedAt());
    }

    @Test
    void sendSms_WhenBlocked_ThrowsExceptionAndWritesNothing() {
        SmsRequest request = new SmsRequest("+1234567890", "Test message");
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(setOperations.isMember("blocked_users", "+1234567890")).thenReturn(true);

        assertThrows(BlockedUserException.class, () -> smsService.sendSms(request));

        // Blocked numbers never reach the outbox.
        verifyNoInteractions(outboxRepository);
    }
}
