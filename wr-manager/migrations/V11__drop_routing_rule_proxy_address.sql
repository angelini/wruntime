-- Item 1: RoutingRule.proxy_address removed from the proto/routing contract.
-- The column is now dead; drop it. wr_engines.proxy_address is a separate
-- concept (EngineRegistration.proxy_address) and is intentionally left in place.
ALTER TABLE wr_routing_rules DROP COLUMN IF EXISTS proxy_address;
