package com.meesho.sms.outbox;

import com.meesho.sms.event.SmsEvent;
import com.meesho.sms.event.SmsEventPublisher;
import com.meesho.sms.vendor.SmsVendorService;
import com.meesho.sms.vendor.VendorResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

/**
 * Background worker that drains the outbox in two distinct steps.
 *
 *   PENDING → call vendor → SENT
 *   SENT    → publish to Kafka → PUBLISHED
 *
 * Both steps run on a single scheduler thread (default @Scheduled pool size 1),
 * so no two ticks process the same row concurrently. Because the steps are
 * separate, a Kafka outage only re-runs the publish step — the vendor is never
 * called twice, and the idempotency key is a second line of defence even if the
 * worker crashes between the vendor call and the status write.
 *
 * Failures are retried with exponential backoff (a row is only picked up once
 * its nextAttemptAt has elapsed). After max-attempts the row is dead-lettered
 * (FAILED) so a genuinely stuck row stops consuming cycles — while a transient
 * outage is ridden out because the backoff window spans far longer than any
 * normal blip.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxProcessor {

    private final OutboxRepository outboxRepository;
    private final SmsVendorService vendorService;
    private final SmsEventPublisher eventPublisher;

    @Value("${outbox.max-attempts:10}")
    private int maxAttempts;

    @Value("${outbox.backoff-base-ms:2000}")
    private long backoffBaseMs;

    @Value("${outbox.backoff-max-ms:300000}")
    private long backoffMaxMs;

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}")
    public void process() {
        // 'now' is captured fresh for each step so a row promoted to SENT during
        // processPending is immediately eligible for publishing in the SAME tick —
        // otherwise it would wait a whole extra poll interval between each transition.
        processPending(Instant.now());
        processSent(Instant.now());
    }

    /** Step 1: call the vendor for accepted-but-unsent requests that are due. */
    private void processPending(Instant now) {
        List<OutboxEvent> due = outboxRepository
                .findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByCreatedAtAsc(OutboxStatus.PENDING, now);
        for (OutboxEvent entry : due) {
            try {
                // Idempotency key = row id → the vendor never double-sends.
                VendorResponse vr = vendorService.send(entry.getId(), entry.getPhoneNumber(), entry.getMessage());

                entry.setVendorStatus(vr.isSuccess() ? "SUCCESS" : "FAILED");
                entry.setVendorMessageId(vr.getVendorMessageId());
                entry.setErrorReason(vr.getErrorReason());
                advanceTo(entry, OutboxStatus.SENT);

                log.info("Outbox {} → vendor done ({}), now SENT", entry.getId(), entry.getVendorStatus());
            } catch (Exception e) {
                // Vendor is idempotent, so retrying after a partial failure here
                // can never produce a duplicate SMS.
                recordFailure(entry, "vendor call", e);
            }
        }
    }

    /** Step 2: publish the recorded result to Kafka for due rows. */
    private void processSent(Instant now) {
        List<OutboxEvent> due = outboxRepository
                .findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByCreatedAtAsc(OutboxStatus.SENT, now);
        for (OutboxEvent entry : due) {
            SmsEvent event = SmsEvent.builder()
                    .eventId(entry.getId())   // stable across republishes → consumer dedups on this
                    .phoneNumber(entry.getPhoneNumber())
                    .message(entry.getMessage())
                    .status(entry.getVendorStatus())
                    .vendorMessageId(entry.getVendorMessageId())
                    .errorReason(entry.getErrorReason())
                    .timestamp(OffsetDateTime.ofInstant(entry.getCreatedAt(), ZoneId.systemDefault()))
                    .build();
            try {
                eventPublisher.publish(event);   // blocking + throws on failure (worker thread)
                advanceTo(entry, OutboxStatus.PUBLISHED);
                log.info("Outbox {} → published to Kafka, now PUBLISHED", entry.getId());
            } catch (Exception e) {
                // Stays SENT (or dead-letters); the vendor is NOT called again.
                recordFailure(entry, "kafka publish", e);
            }
        }
    }

    /** Move a row to its next status with a fresh retry budget. */
    private void advanceTo(OutboxEvent entry, OutboxStatus next) {
        entry.setStatus(next);
        entry.setAttempts(0);
        entry.setNextAttemptAt(Instant.now());
        entry.setLastError(null);
        entry.setUpdatedAt(Instant.now());
        outboxRepository.save(entry);
    }

    /**
     * Count the failed attempt, then either schedule an exponential-backoff retry
     * or — once attempts cross the cap — dead-letter the row.
     */
    private void recordFailure(OutboxEvent entry, String stage, Exception e) {
        int attempts = entry.getAttempts() + 1;
        entry.setAttempts(attempts);
        entry.setLastError(stage + ": " + e.getMessage());
        entry.setUpdatedAt(Instant.now());

        if (attempts >= maxAttempts) {
            entry.setStatus(OutboxStatus.FAILED);
            log.error("Outbox {} dead-lettered after {} attempts at [{}]: {}",
                    entry.getId(), attempts, stage, e.getMessage());
        } else {
            long delayMs = backoffDelayMs(attempts);
            entry.setNextAttemptAt(Instant.now().plusMillis(delayMs));
            log.warn("Outbox {} failed at [{}] (attempt {}/{}), retrying in {} ms: {}",
                    entry.getId(), stage, attempts, maxAttempts, delayMs, e.getMessage());
        }
        outboxRepository.save(entry);
    }

    /** base * 2^(attempts-1), capped at backoffMaxMs. */
    private long backoffDelayMs(int attempts) {
        long factor = 1L << Math.min(attempts - 1, 30);
        long delay = backoffBaseMs * factor;
        return Math.min(delay, backoffMaxMs);
    }
}
