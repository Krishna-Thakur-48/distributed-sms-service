package com.meesho.sms.controller;

import com.meesho.sms.dto.BlockResponse;
import com.meesho.sms.dto.SmsRequest;
import com.meesho.sms.dto.SmsResponse;
import com.meesho.sms.service.SmsService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/v1/sms")
@RequiredArgsConstructor
public class SmsController {
    private final SmsService smsService;

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    @PostMapping("/send")
    public ResponseEntity<SmsResponse> sendSms(@Valid @RequestBody SmsRequest request) {
        // 202 Accepted: the request is durably queued in the outbox. The actual
        // delivery + Kafka publish happen asynchronously; the final result shows
        // up in the store service's message history.
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(smsService.sendSms(request));
    }

    @GetMapping("/block/{phoneNumber}")
    public ResponseEntity<BlockResponse> checkIfBlocked(@PathVariable String phoneNumber) {
        return ResponseEntity.ok(smsService.checkIfBlocked(phoneNumber));
    }

    @PostMapping("/block/{phoneNumber}")
    public ResponseEntity<BlockResponse> blockUser(@PathVariable String phoneNumber) {
        return ResponseEntity.ok(smsService.blockUser(phoneNumber));
    }

    @DeleteMapping("/block/{phoneNumber}")
    public ResponseEntity<BlockResponse> unblockUser(@PathVariable String phoneNumber) {
        return ResponseEntity.ok(smsService.unblockUser(phoneNumber));
    }
}
