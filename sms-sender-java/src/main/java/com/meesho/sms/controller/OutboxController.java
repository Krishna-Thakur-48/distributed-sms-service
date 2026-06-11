package com.meesho.sms.controller;

import com.meesho.sms.outbox.OutboxEvent;
import com.meesho.sms.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Admin endpoints for the dead-letter queue.
 *   GET  /v1/sms/outbox/failed        → list quarantined rows
 *   POST /v1/sms/outbox/{id}/replay   → requeue one row
 */
@RestController
@RequestMapping("/v1/sms/outbox")
@RequiredArgsConstructor
public class OutboxController {

    private final OutboxService outboxService;

    @GetMapping
    public ResponseEntity<List<OutboxEvent>> listRecent() {
        return ResponseEntity.ok(outboxService.listRecent());
    }

    @GetMapping("/failed")
    public ResponseEntity<List<OutboxEvent>> listFailed() {
        return ResponseEntity.ok(outboxService.listFailed());
    }

    @PostMapping("/{id}/replay")
    public ResponseEntity<OutboxEvent> replay(@PathVariable String id) {
        return ResponseEntity.ok(outboxService.replay(id));
    }
}
