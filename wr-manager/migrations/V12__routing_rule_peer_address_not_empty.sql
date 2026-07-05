-- Item 2 (defense-in-depth for the manager-boundary guard): peer_address is the
-- sole cross-node forwarding address and must be non-empty. Empty-peer rules are
-- already unroutable (the proxy skips them at index construction); remove any
-- legacy rows before enforcing the invariant so the constraint validates cleanly.
DELETE FROM wr_routing_rules WHERE peer_address = '';
ALTER TABLE wr_routing_rules
    ADD CONSTRAINT wr_routing_rules_peer_address_not_empty CHECK (peer_address <> '');
