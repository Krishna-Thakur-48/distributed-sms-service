package com.meesho.sms.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.sms.exception.ErrorResponse;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.LocalDateTime;

/**
 * Guards the admin surface with a shared API key.
 *
 * Protected:  /v1/sms/block/**   (block / unblock / check)
 *             /v1/sms/outbox/**  (list / failed / replay)
 * Open:       /v1/sms/send, /v1/sms/health
 *
 * The key is expected in the X-API-Key header. In the demo the reverse proxy
 * injects it for browser traffic, so the secret never lives in client code —
 * the proxy acts as a trusted gateway. Hitting the service directly without the
 * header returns 401.
 */
@Component
@RequiredArgsConstructor
public class ApiKeyAuthFilter extends OncePerRequestFilter {

    private static final String API_KEY_HEADER = "X-API-Key";

    private final ObjectMapper objectMapper;

    @Value("${admin.api-key}")
    private String expectedApiKey;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        if (requiresAuth(request.getServletPath())) {
            String provided = request.getHeader(API_KEY_HEADER);
            if (provided == null || !provided.equals(expectedApiKey)) {
                writeUnauthorized(response);
                return;   // short-circuit — do not forward to the controller
            }
        }
        chain.doFilter(request, response);
    }

    private boolean requiresAuth(String path) {
        return path.startsWith("/v1/sms/block") || path.startsWith("/v1/sms/outbox");
    }

    private void writeUnauthorized(HttpServletResponse response) throws IOException {
        ErrorResponse body = ErrorResponse.builder()
                .timestamp(LocalDateTime.now())
                .status(HttpStatus.UNAUTHORIZED.value())
                .error(HttpStatus.UNAUTHORIZED.getReasonPhrase())
                .message("Missing or invalid API key")
                .build();

        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        objectMapper.writeValue(response.getWriter(), body);
    }
}
