CREATE TABLE IF inventory (
    product_id TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    stock      BIGINT NOT NULL CHECK (stock >= 0)
);
