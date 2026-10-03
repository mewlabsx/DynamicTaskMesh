CREATE TABLE IF NOT EXISTS tasks (
	task_id TEXT PRIMARY KEY,
	intent TEXT NOT NULL,
	requirements_json TEXT NOT NULL,
	constraints_json TEXT NOT NULL,
	status TEXT NOT NULL,
	execution_status TEXT NOT NULL DEFAULT '',
	execution_error TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_steps (
	task_id TEXT NOT NULL,
	step_id TEXT NOT NULL,
	position INTEGER NOT NULL,
	capability TEXT NOT NULL,
	node_id TEXT NOT NULL DEFAULT '',
	inputs_json TEXT NOT NULL,
	state TEXT NOT NULL DEFAULT 'created',
	status TEXT NOT NULL DEFAULT '',
	output_json TEXT NOT NULL,
	error TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (task_id, step_id),
	FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS execution_steps_task_position
	ON execution_steps(task_id, position);
