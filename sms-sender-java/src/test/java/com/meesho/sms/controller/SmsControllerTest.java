package com.meesho.sms.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.sms.dto.SmsRequest;
import com.meesho.sms.dto.SmsResponse;
import com.meesho.sms.exception.BlockedUserException;
import com.meesho.sms.service.SmsService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SmsController.class)
public class SmsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SmsService smsService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void sendSms_ValidRequest_ReturnsSuccess() throws Exception {
        SmsRequest request = new SmsRequest("+1234567890", "Test message");
        SmsResponse response = SmsResponse.builder()
                .status("SUCCESS")
                .messageId("123-abc")
                .message("SMS sent successfully to +1234567890")
                .build();

        Mockito.when(smsService.sendSms(any(SmsRequest.class))).thenReturn(response);

        mockMvc.perform(post("/v1/sms/send")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.messageId").value("123-abc"));
    }

    @Test
    void sendSms_InvalidPhoneNumber_ReturnsBadRequest() throws Exception {
        SmsRequest request = new SmsRequest("invalid-phone", "Test message");

        mockMvc.perform(post("/v1/sms/send")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.validationErrors.phoneNumber").exists());
    }

    @Test
    void sendSms_BlockedUser_ReturnsForbidden() throws Exception {
        SmsRequest request = new SmsRequest("+1234567890", "Test message");

        Mockito.when(smsService.sendSms(any(SmsRequest.class)))
                .thenThrow(new BlockedUserException("Phone number is blocked from receiving SMS"));

        mockMvc.perform(post("/v1/sms/send")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error").value("Forbidden"))
                .andExpect(jsonPath("$.message").value("Phone number is blocked from receiving SMS"));
    }
}
