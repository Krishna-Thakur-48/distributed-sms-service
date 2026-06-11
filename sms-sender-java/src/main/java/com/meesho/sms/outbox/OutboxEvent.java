package com.meesho.sms.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * A durable record of one SMS request. Written synchronously when a request is
 * accepted, then processed asynchronously by {@link OutboxProcessor}.
 */
@Entity
@Table(name = "outbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    /** UUID. Also used as the idempotency key passed to the vendor. */
    @Id
    private String id;

    @Column(nullable = false)
    private String phoneNumber;

    @Column(nullable = false, length = 1000)
    private String message;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    /** Vendor outcome — populated when the row moves PENDING → SENT. */
    private String vendorStatus;     // SUCCESS / FAILED
    private String vendorMessageId;
    private String errorReason;      // vendor business error (e.g. "Carrier rejected")

    /** Failed attempts at the CURRENT step. Reset to 0 on each step transition. */
    @Column(nullable = false)
    private int attempts;

    /** Earliest time this row is eligible for processing (drives exponential backoff). */
    @Column(nullable = false)
    private Instant nextAttemptAt;

    /** Last infrastructure failure (exception) — for dead-letter inspection. */
    @Column(length = 500)
    private String lastError;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;
}
