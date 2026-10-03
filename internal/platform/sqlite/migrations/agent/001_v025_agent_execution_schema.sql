CREATE TABLE IF NOT EXISTS executions (
	execution_id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL,
	step_id TEXT NOT NULL,
	idempotency_key TEXT NOT NULL UNIQUE,
	fingerprint_json TEXT NOT NULL,
	status TEXT NOT NULL,
	result_json TEXT NOT NULL DEFAULT '{}',
	execution_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS executions_task_step
	ON executions(task_id, step_id);
