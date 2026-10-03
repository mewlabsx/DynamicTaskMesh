CREATE TABLE resources (
	resource_id TEXT PRIMARY KEY
		CHECK (length(trim(resource_id)) > 0),
	owner_node_id TEXT NOT NULL
		CHECK (length(trim(owner_node_id)) > 0),
	resource_kind TEXT NOT NULL
		CHECK (resource_kind = 'capability'),
	resource_type TEXT NOT NULL
		CHECK (length(resource_type) > 0),
	resource_generation TEXT NOT NULL
		CHECK (
			typeof(resource_generation) = 'text'
			AND length(resource_generation) BETWEEN 1 AND 20
			AND resource_generation NOT GLOB '*[^0-9]*'
			AND resource_generation <> '0'
			AND (length(resource_generation) = 1 OR substr(resource_generation, 1, 1) <> '0')
			AND (length(resource_generation) < 20 OR resource_generation <= '18446744073709551615')
		),
	owner_node_generation INTEGER NOT NULL
		CHECK (owner_node_generation >= 1),
	registration_id TEXT NOT NULL
		CHECK (length(registration_id) > 0 AND registration_id = trim(registration_id)),
	publication_state TEXT NOT NULL
		CHECK (publication_state IN ('published', 'withdrawn')),
	descriptor_encoding_version INTEGER NOT NULL
		CHECK (descriptor_encoding_version = 1),
	operations_json TEXT NOT NULL
		CHECK (json_valid(operations_json) AND json_type(operations_json) = 'array'),
	attributes_json TEXT NOT NULL
		CHECK (json_valid(attributes_json) AND json_type(attributes_json) = 'object'),
	created_at INTEGER NOT NULL
		CHECK (created_at > 0),
	updated_at INTEGER NOT NULL
		CHECK (updated_at > 0 AND updated_at >= created_at),
	FOREIGN KEY (owner_node_id) REFERENCES nodes(node_id) ON DELETE RESTRICT
);

CREATE INDEX idx_resources_owner_state_id
	ON resources(owner_node_id, publication_state, resource_id);

CREATE INDEX idx_resources_owner_fence
	ON resources(owner_node_id, owner_node_generation, registration_id);
