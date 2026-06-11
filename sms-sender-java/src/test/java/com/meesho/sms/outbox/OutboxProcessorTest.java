package com.meesho.sms.outbox;

import com.meesho.sms.event.SmsEventPublisher;
import com.meesho.sms.exception.KafkaPublishException;
import com.meesho.sms.vendor.SmsVendorService;
import com.meesho.sms.vendor.VendorResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OutboxProcessorTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private SmsVendorService vendorService;

    @Mock
    private SmsEventPublisher eventPublisher;

    @InjectMocks
    private OutboxProcessor processor;

    @BeforeEach
    void setUp() {
        // @Value fields aren't populated without a Spring context.
        ReflectionTestUtils.setField(processor, "maxAttempts", 3);
        ReflectionTestUtils.setField(processor, "backoffBaseMs", 1000L);
        ReflectionTestUtils.setField(processor, "backoffMaxMs", 60000L);
    }

    private OutboxEvent row(OutboxStatus status) {
        Instant now = Instant.now();
        return OutboxEvent.builder()
                .id("row-1")
                .phoneNumber("+1234567890")
                .message("hello")
                .status(status)
                .attempts(0)
                .nextAttemptAt(now)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private void duePending(List<OutboxEvent> rows) {
        when(outboxRepository.findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByCreatedAtAsc(
                eq(OutboxStatus.PENDING), any(Instant.class))).thenReturn(rows);
    }

    private void dueSent(List<OutboxEvent> rows) {
        when(outboxRepository.findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByCreatedAtAsc(
                eq(OutboxStatus.SENT), any(Instant.class))).thenReturn(rows);
    }

    @Test
    void happyPath_PendingRowIsSentThenPublished() {
        OutboxEvent entry = row(OutboxStatus.PENDING);
        duePending(List.of(entry));
        dueSent(List.of(entry)); // same row, now SENT after step 1
        when(vendorService.send("row-1", "+1234567890", "hello"))
                .thenReturn(VendorResponse.builder().success(true).vendorMessageId("VND-1").build());

        processor.process();

        verify(vendorService, times(1)).send("row-1", "+1234567890", "hello");
        verify(eventPublisher, times(1)).publish(any());
        assertEquals(OutboxStatus.PUBLISHED, entry.getStatus());
        assertEquals("SUCCESS", entry.getVendorStatus());
        assertEquals(0, entry.getAttempts());
    }

    @Test
    void kafkaDown_SentRowStaysSent_BacksOff_AndVendorNeverCalledAgain() {
        OutboxEvent entry = row(OutboxStatus.SENT);
        entry.setVendorStatus("SUCCESS");
        entry.setVendorMessageId("VND-1");

        duePending(emptyList());
        dueSent(List.of(entry));
        doThrow(new KafkaPublishException("Kafka unavailable", null)).when(eventPublisher).publish(any());

        Instant before = Instant.now();
        processor.process();

        // THE GUARANTEE: a publish failure must never re-contact the vendor.
        verifyNoInteractions(vendorService);
        // Row stays SENT, attempt counted, and a future retry is scheduled (backoff).
        assertEquals(OutboxStatus.SENT, entry.getStatus());
        assertEquals(1, entry.getAttempts());
        assertTrue(entry.getNextAttemptAt().isAfter(before), "backoff should push nextAttemptAt into the future");
        assertNotNull(entry.getLastError());
    }

    @Test
    void publishKeepsFailing_RowIsDeadLetteredAfterMaxAttempts() {
        OutboxEvent entry = row(OutboxStatus.SENT);
        entry.setVendorStatus("SUCCESS");
        entry.setVendorMessageId("VND-1");
        entry.setAttempts(2); // maxAttempts = 3, so the next failure tips it over

        duePending(emptyList());
        dueSent(List.of(entry));
        doThrow(new KafkaPublishException("Kafka unavailable", null)).when(eventPublisher).publish(any());

        processor.process();

        assertEquals(OutboxStatus.FAILED, entry.getStatus());
        assertEquals(3, entry.getAttempts());
        assertNotNull(entry.getLastError());
        verifyNoInteractions(vendorService);
    }
}
