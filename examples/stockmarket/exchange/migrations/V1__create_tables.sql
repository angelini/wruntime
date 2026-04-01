CREATE TABLE orders (
    order_id   BIGSERIAL PRIMARY KEY,
    trader_id  TEXT    NOT NULL,
    symbol     TEXT    NOT NULL,
    is_buy     BOOLEAN NOT NULL,
    price      BIGINT  NOT NULL,
    quantity   BIGINT  NOT NULL,
    remaining  BIGINT  NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_match
    ON orders (symbol, is_buy, price, created_at)
    WHERE remaining > 0;

CREATE TABLE positions (
    trader_id  TEXT   NOT NULL,
    symbol     TEXT   NOT NULL,
    shares     BIGINT NOT NULL DEFAULT 0,
    cash_flow  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (trader_id, symbol)
);
