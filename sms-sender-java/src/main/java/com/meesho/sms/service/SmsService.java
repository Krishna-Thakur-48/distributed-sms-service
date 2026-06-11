package com.meesho.sms.service;

import com.meesho.sms.dto.BlockResponse;
import com.meesho.sms.dto.SmsRequest;
import com.meesho.sms.dto.SmsResponse;

public interface SmsService {
    SmsResponse sendSms(SmsRequest request);
    BlockResponse blockUser(String phoneNumber);
    BlockResponse unblockUser(String phoneNumber);
    BlockResponse checkIfBlocked(String phoneNumber);
}
