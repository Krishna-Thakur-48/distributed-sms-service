package com.meesho.sms.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Admin operations over dead-lettered outbox rows: inspect them, and requeue
 * one for another run once the underlying cause is fixed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxService {

    private final OutboxRepository outboxRepository;

    public List<OutboxEvent> listFailed() {
        return outboxRepository.findByStatusOrderByUpdatedAtDesc(OutboxStatus.FAILED);
    }

    /** Most recent rows across all statuses — for the live outbox monitor. */
    public List<OutboxEvent> listRecent() {
        return outboxRepository.findTop100ByOrderByCreatedAtDesc();
    }

    /**
     * Requeue a dead-lettered row. It resumes from the correct step so the
     * no-double-send guarantee holds:
     *   - vendor already succeeded (vendorStatus set) → resume at SENT (publish only)
     *   - vendor never completed (vendorStatus null)  → restart at PENDING
     */
    public OutboxEvent replay(String id) {
        OutboxEvent entry = outboxRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("No outbox row with id " + id));

        if (entry.getStatus() != OutboxStatus.FAILED) {
            throw new IllegalStateException(
                    "Only FAILED rows can be replayed; row " + id + " is " + entry.getStatus());
        }

        OutboxStatus resumeAt = entry.getVendorStatus() != null ? OutboxStatus.SENT : OutboxStatus.PENDING;
        Instant now = Instant.now();
        entry.setStatus(resumeAt);
        entry.setAttempts(0);
        entry.setNextAttemptAt(now);
        entry.setLastError(null);
        entry.setUpdatedAt(now);

        log.info("Outbox {} replayed → resuming at {}", id, resumeAt);
        return outboxRepository.save(entry);
    }
}
