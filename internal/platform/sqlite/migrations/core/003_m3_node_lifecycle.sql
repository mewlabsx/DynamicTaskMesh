ALTER TABLE nodes ADD COLUMN registration_id TEXT NOT NULL DEFAULT '';
ALTER TABLE nodes ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE nodes ADD COLUMN registered_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN last_heartbeat_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN lease_expires_at INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_nodes_status_lease_expiry
	ON nodes(status, lease_expires_at, node_id);
