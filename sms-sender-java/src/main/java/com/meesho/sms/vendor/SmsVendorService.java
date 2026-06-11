package com.meesho.sms.vendor;

public interface SmsVendorService {

    /**
     * @param idempotencyKey unique per SMS request. The vendor must not contact
     *                       the carrier more than once for the same key, so a
     *                       retry never double-messages the receiver.
     */
    VendorResponse send(String idempotencyKey, String phoneNumber, String message);
}
