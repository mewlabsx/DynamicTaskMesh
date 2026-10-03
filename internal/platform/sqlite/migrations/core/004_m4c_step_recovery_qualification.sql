ALTER TABLE task_steps
ADD COLUMN idempotency_mode TEXT NOT NULL DEFAULT 'unspecified'
CHECK (idempotency_mode IN ('unspecified', 'idempotent', 'non_idempotent'));

CREATE INDEX idx_task_steps_recovery
	ON task_steps(state, idempotency_mode, task_id, sequence_no);
