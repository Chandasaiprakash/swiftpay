package com.swiftpay.gateway.event;

public final class KafkaTopics {

    private KafkaTopics() {
    }

    public static final String PAYMENTS_INITIATED =
            "payments.initiated";

    public static final String PAYMENTS_COMPLETED =
            "payments.completed";

    public static final String PAYMENTS_FAILED =
            "payments.failed";

    public static final String PAYMENTS_DLT =
            "payments.dlt";
}