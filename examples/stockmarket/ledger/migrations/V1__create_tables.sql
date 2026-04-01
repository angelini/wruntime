CREATE TABLE trades (
    trade_id    BIGSERIAL PRIMARY KEY,
    buyer_id    TEXT    NOT NULL,
    seller_id   TEXT    NOT NULL,
    symbol      TEXT    NOT NULL,
    quantity    BIGINT  NOT NULL,
    price       BIGINT  NOT NULL,
    order_id    BIGINT  NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
