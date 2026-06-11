package com.meesho.sms.vendor.impl;

import com.meesho.sms.vendor.SmsVendorService;
import com.meesho.sms.vendor.VendorResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class MockSmsVendorService implements SmsVendorService {

    /**
     * Remembers the result for every idempotency key we've already handled.
     * A real vendor enforces this server-side; we simulate it in-memory so the
     * demo proves the guarantee: the same key never sends a second SMS.
     */
    private final Map<String, VendorResponse> resultsByKey = new ConcurrentHashMap<>();

    @Override
    public VendorResponse send(String idempotencyKey, String phoneNumber, String message) {
        VendorResponse cached = resultsByKey.get(idempotencyKey);
        if (cached != null) {
            log.warn("Idempotent replay for key {} — NOT re-sending to {}", idempotencyKey, phoneNumber);
            return cached;
        }

        log.info("Mock vendor sending SMS to {} (key {})", phoneNumber, idempotencyKey);

        VendorResponse response;
        // Deterministic failure simulation
        if ("+9999999999".equals(phoneNumber) || "9999999999".equals(phoneNumber)) {
            log.warn("Mock vendor simulated failure for {}", phoneNumber);
            response = VendorResponse.builder()
                    .success(false)
                    .errorReason("Carrier rejected message")
                    .build();
        } else {
            String vendorMessageId = "VND-" + UUID.randomUUID();
            log.info("Mock vendor successfully sent SMS. Vendor ID: {}", vendorMessageId);
            response = VendorResponse.builder()
                    .success(true)
                    .vendorMessageId(vendorMessageId)
                    .build();
        }

        resultsByKey.put(idempotencyKey, response);
        return response;
    }
}
