CREATE TABLE accounts (
                          user_id BIGINT PRIMARY KEY,
                          balance NUMERIC(19,2) NOT NULL CHECK (balance >= 0),
                          created_at TIMESTAMP NOT NULL,
                          updated_at TIMESTAMP NOT NULL
);

CREATE TABLE processed_events (
                                  event_id UUID PRIMARY KEY,
                                  processed_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_processed_events
    ON processed_events(processed_at);

INSERT INTO accounts (
    user_id,
    balance,
    created_at,
    updated_at
)
VALUES
    (1001, 10000.00, NOW(), NOW()),
    (2005, 5000.00, NOW(), NOW()),
    (3001, 7000.00, NOW(), NOW());