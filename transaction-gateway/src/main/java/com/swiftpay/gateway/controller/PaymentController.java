package com.swiftpay.gateway.controller;

import com.swiftpay.gateway.dto.PaymentRequest;
import com.swiftpay.gateway.dto.PaymentResponse;
import com.swiftpay.gateway.dto.PaymentStatusResponse;
import com.swiftpay.gateway.service.PaymentQueryService;
import com.swiftpay.gateway.service.PaymentService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

@RestController
@RequestMapping("/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentQueryService paymentQueryService;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PaymentResponse initiatePayment(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody PaymentRequest request
    ) {

        return paymentService.initiatePayment(
                idempotencyKey,
                request
        );
    }
    @GetMapping("/{transactionId}")
    public ResponseEntity<PaymentStatusResponse>
    getPaymentStatus(
            @PathVariable UUID transactionId
    ) {

        return ResponseEntity.ok(
                paymentQueryService.getPaymentStatus(
                        transactionId
                )
        );
    }
}