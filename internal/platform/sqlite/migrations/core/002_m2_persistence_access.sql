DROP TABLE IF EXISTS execution_steps;
DROP TABLE IF EXISTS tasks;

CREATE TABLE tasks (
	task_id TEXT PRIMARY KEY,
	intent TEXT NOT NULL,
	requirements_json TEXT NOT NULL,
	constraints_json TEXT NOT NULL,
	status TEXT NOT NULL,
	failure_code TEXT,
	failure_message TEXT,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	started_at INTEGER,
	completed_at INTEGER,
	version INTEGER NOT NULL CHECK (version > 0)
);

CREATE TABLE task_steps (
	task_id TEXT NOT NULL,
	step_id TEXT NOT NULL,
	sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
	capability TEXT NOT NULL,
	input_json TEXT NOT NULL,
	state TEXT NOT NULL,
	assigned_node_id TEXT,
	attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
	max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
	failure_code TEXT,
	failure_message TEXT,
	result_json TEXT,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	started_at INTEGER,
	completed_at INTEGER,
	version INTEGER NOT NULL CHECK (version > 0),
	PRIMARY KEY (task_id, step_id),
	UNIQUE (task_id, sequence_no),
	FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE TABLE executions (
	execution_id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL,
	step_id TEXT NOT NULL,
	attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
	node_id TEXT NOT NULL,
	status TEXT NOT NULL,
	request_json TEXT NOT NULL,
	result_json TEXT,
	failure_code TEXT,
	failure_message TEXT,
	started_at INTEGER,
	completed_at INTEGER,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	version INTEGER NOT NULL CHECK (version > 0),
	UNIQUE (task_id, step_id, attempt_no),
	FOREIGN KEY (task_id, step_id) REFERENCES task_steps(task_id, step_id) ON DELETE CASCADE
);

CREATE TABLE task_events (
	event_id INTEGER PRIMARY KEY AUTOINCREMENT,
	task_id TEXT NOT NULL,
	step_id TEXT,
	event_type TEXT NOT NULL,
	from_state TEXT,
	to_state TEXT,
	detail_json TEXT,
	created_at INTEGER NOT NULL,
	FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
	FOREIGN KEY (task_id, step_id) REFERENCES task_steps(task_id, step_id)
);

CREATE TABLE nodes (
	node_id TEXT PRIMARY KEY,
	endpoint TEXT NOT NULL,
	capabilities_json TEXT NOT NULL,
	status TEXT NOT NULL,
	generation INTEGER NOT NULL CHECK (generation > 0),
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);

CREATE INDEX idx_tasks_status_updated
	ON tasks(status, updated_at, task_id);
CREATE INDEX idx_task_steps_task_sequence
	ON task_steps(task_id, sequence_no);
CREATE INDEX idx_executions_step_attempt
	ON executions(task_id, step_id, attempt_no);
CREATE INDEX idx_executions_started
	ON executions(started_at, attempt_no, execution_id);
CREATE INDEX idx_task_events_task_created
	ON task_events(task_id, created_at, event_id);
