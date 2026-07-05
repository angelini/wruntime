DELETE FROM wr_routing_rules WHERE peer_address = '';
ALTER TABLE wr_routing_rules
    ADD CONSTRAINT wr_routing_rules_peer_address_not_empty CHECK (peer_address <> '');
