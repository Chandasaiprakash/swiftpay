package com.swiftpay.analytics.controller;

import com.swiftpay.analytics.dto.AnalyticsResponse;
import com.swiftpay.analytics.service.AnalyticsQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsQueryService
            analyticsQueryService;

    @GetMapping
    public ResponseEntity<
            List<AnalyticsResponse>
            > getAnalytics() {

        return ResponseEntity.ok(
                analyticsQueryService
                        .getAllMetrics()
        );
    }
}