CREATE TABLE transactions (
                              transaction_id UUID PRIMARY KEY,
                              sender_id BIGINT NOT NULL,
                              receiver_id BIGINT NOT NULL,
                              amount NUMERIC(19,2) NOT NULL,
                              currency VARCHAR(3) NOT NULL,
                              status VARCHAR(20) NOT NULL,
                              created_at TIMESTAMP NOT NULL,
                              updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_transactions_sender
    ON transactions(sender_id);

CREATE INDEX idx_transactions_receiver
    ON transactions(receiver_id);

CREATE INDEX idx_transactions_status
    ON transactions(status);

CREATE TABLE transaction_outbox (
                                    id UUID PRIMARY KEY,
                                    aggregate_id UUID NOT NULL,
                                    event_type VARCHAR(100) NOT NULL,
                                    payload JSONB NOT NULL,
                                    status VARCHAR(20) NOT NULL,
                                    created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_outbox_status
    ON transaction_outbox(status);