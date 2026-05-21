package com.meesho.sms.vendor.impl;

import com.meesho.sms.vendor.SmsVendorService;
import com.meesho.sms.vendor.VendorResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
public class MockSmsVendorService implements SmsVendorService {

    @Override
    public VendorResponse send(String phoneNumber, String message) {
        log.info("Mock vendor attempting to send SMS to {}", phoneNumber);
        
        // Deterministic failure simulation
        if ("+9999999999".equals(phoneNumber) || "9999999999".equals(phoneNumber)) {
            log.warn("Mock vendor simulated failure for {}", phoneNumber);
            return VendorResponse.builder()
                    .success(false)
                    .errorReason("Carrier rejected message")
                    .build();
        }

        // Success simulation
        String vendorMessageId = "VND-" + UUID.randomUUID().toString();
        log.info("Mock vendor successfully sent SMS. Vendor ID: {}", vendorMessageId);
        
        return VendorResponse.builder()
                .success(true)
                .vendorMessageId(vendorMessageId)
                .build();
    }
}
