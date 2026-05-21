package com.meesho.sms.vendor;

public interface SmsVendorService {
    VendorResponse send(String phoneNumber, String message);
}
