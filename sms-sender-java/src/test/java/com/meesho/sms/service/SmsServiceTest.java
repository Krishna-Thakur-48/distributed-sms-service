package com.meesho.sms.service;

import com.meesho.sms.dto.SmsRequest;
import com.meesho.sms.dto.SmsResponse;
import com.meesho.sms.event.SmsEvent;
import com.meesho.sms.event.SmsEventPublisher;
import com.meesho.sms.exception.BlockedUserException;
import com.meesho.sms.service.impl.SmsServiceImpl;
import com.meesho.sms.vendor.SmsVendorService;
import com.meesho.sms.vendor.VendorResponse;
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
    private SmsVendorService vendorService;

    @Mock
    private SmsEventPublisher eventPublisher;

    @Captor
    private ArgumentCaptor<SmsEvent> eventCaptor;

    private SmsService smsService;

    @BeforeEach
    void setUp() {
        smsService = new SmsServiceImpl(redisTemplate, vendorService, eventPublisher);
    }

    @Test
    void sendSms_WhenNotBlockedAndVendorSuccess_PublishesSuccessEvent() {
        SmsRequest request = new SmsRequest("+1234567890", "Test message");
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(setOperations.isMember("blocked_users", "+1234567890")).thenReturn(false);

        VendorResponse vendorResponse = VendorResponse.builder()
                .success(true)
                .vendorMessageId("VND-123")
                .build();
        when(vendorService.send("+1234567890", "Test message")).thenReturn(vendorResponse);

        SmsResponse response = smsService.sendSms(request);

        assertNotNull(response);
        assertEquals("SUCCESS", response.getStatus());

        verify(eventPublisher).publish(eventCaptor.capture());
        SmsEvent publishedEvent = eventCaptor.getValue();
        assertEquals("+1234567890", publishedEvent.getPhoneNumber());
        assertEquals("SUCCESS", publishedEvent.getStatus());
        assertEquals("VND-123", publishedEvent.getVendorMessageId());
        assertNull(publishedEvent.getErrorReason());
        assertNotNull(publishedEvent.getTimestamp());
    }

    @Test
    void sendSms_WhenNotBlockedAndVendorFails_PublishesFailedEvent() {
        SmsRequest request = new SmsRequest("+9999999999", "Test message");
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(setOperations.isMember("blocked_users", "+9999999999")).thenReturn(false);

        VendorResponse vendorResponse = VendorResponse.builder()
                .success(false)
                .errorReason("Carrier rejected message")
                .build();
        when(vendorService.send("+9999999999", "Test message")).thenReturn(vendorResponse);

        SmsResponse response = smsService.sendSms(request);

        assertNotNull(response);
        assertEquals("FAILED", response.getStatus());

        verify(eventPublisher).publish(eventCaptor.capture());
        SmsEvent publishedEvent = eventCaptor.getValue();
        assertEquals("+9999999999", publishedEvent.getPhoneNumber());
        assertEquals("FAILED", publishedEvent.getStatus());
        assertNull(publishedEvent.getVendorMessageId());
        assertEquals("Carrier rejected message", publishedEvent.getErrorReason());
        assertNotNull(publishedEvent.getTimestamp());
    }

    @Test
    void sendSms_WhenBlocked_ThrowsExceptionAndDoesNotPublish() {
        SmsRequest request = new SmsRequest("+1234567890", "Test message");
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(setOperations.isMember("blocked_users", "+1234567890")).thenReturn(true);

        assertThrows(BlockedUserException.class, () -> smsService.sendSms(request));
        
        verifyNoInteractions(vendorService);
        verifyNoInteractions(eventPublisher);
    }
}
