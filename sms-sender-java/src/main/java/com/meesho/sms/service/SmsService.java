package com.meesho.sms.service;

import com.meesho.sms.dto.SmsRequest;
import com.meesho.sms.dto.SmsResponse;

public interface SmsService {
    SmsResponse sendSms(SmsRequest request);
}
