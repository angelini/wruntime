ALTER TABLE wr_engines ADD COLUMN IF NOT EXISTS proxy_address TEXT NOT NULL DEFAULT '';
ALTER TABLE wr_routing_rules ADD COLUMN IF NOT EXISTS proxy_address TEXT NOT NULL DEFAULT '';

UPDATE wr_routing_rules r
SET proxy_address = e.proxy_address
FROM wr_engines e
WHERE r.engine_id = e.engine_id
  AND r.proxy_address = ''
  AND e.proxy_address <> '';
