CREATE TABLE payment_analytics (

                                   id BIGSERIAL PRIMARY KEY,

                                   metric_name VARCHAR(100) NOT NULL,

                                   metric_value NUMERIC(19,2) NOT NULL,

                                   currency VARCHAR(10),

                                   updated_at TIMESTAMP NOT NULL
);