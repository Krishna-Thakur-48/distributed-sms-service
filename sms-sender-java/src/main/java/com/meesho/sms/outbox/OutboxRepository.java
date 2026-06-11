package com.meesho.sms.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Oldest-first batch of rows in a given status that are DUE now (their
     * backoff window has elapsed). Bounded to 100 so a large backlog drains over
     * several cycles rather than blocking one long cycle.
     */
    List<OutboxEvent> findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByCreatedAtAsc(
            OutboxStatus status, Instant now);

    /** Dead-lettered rows, most recently failed first — for the admin endpoint. */
    List<OutboxEvent> findByStatusOrderByUpdatedAtDesc(OutboxStatus status);

    /** Most recent rows across all statuses — powers the live outbox monitor UI. */
    List<OutboxEvent> findTop100ByOrderByCreatedAtDesc();
}
