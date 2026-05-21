package com.meesho.sms.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SmsEvent {
    private String phoneNumber;
    private String message;
    private String status;
    private String vendorMessageId;
    private String errorReason;
    private OffsetDateTime timestamp;
}
