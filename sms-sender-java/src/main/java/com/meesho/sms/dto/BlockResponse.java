package com.meesho.sms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BlockResponse {
    private String phoneNumber;
    private boolean blocked;
    private String message;
}
