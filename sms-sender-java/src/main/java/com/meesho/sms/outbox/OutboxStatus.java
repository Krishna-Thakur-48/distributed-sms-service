package com.meesho.sms.outbox;

/**
 * Lifecycle of an outbox row.
 *
 * PENDING   → request accepted, vendor not yet called
 * SENT      → vendor called, result recorded, not yet published to Kafka
 * PUBLISHED → event published to Kafka (terminal — success)
 * FAILED    → exhausted all retries; quarantined for inspection/replay (terminal)
 *
 * The split between PENDING and SENT is what guarantees the receiver is never
 * messaged twice: once a row is SENT, a Kafka failure only retries the publish
 * step — the vendor is never contacted again.
 */
public enum OutboxStatus {
    PENDING,
    SENT,
    PUBLISHED,
    FAILED
}
