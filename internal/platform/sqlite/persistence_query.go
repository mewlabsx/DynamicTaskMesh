package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"dtm/internal/lifecycle"
	"dtm/internal/model"
	storageport "dtm/internal/storage"
)

const taskPageTokenVersion = 1

type taskPageCursor struct {
	Version       int    `json:"version"`
	CreatedAtNano int64  `json:"created_at_unix_nano"`
	TaskID        string `json:"task_id"`
}

func (repository *Repository) ListTasks(ctx context.Context, filter storageport.TaskFilter) (page storageport.TaskPage, returnErr error) {
	defer func() { returnErr = classifyTaskQueryError(ctx, returnErr) }()
	if err := ctx.Err(); err != nil {
		return storageport.TaskPage{}, err
	}
	if _, err := repository.database.SQLDB(); err != nil {
		return storageport.TaskPage{}, err
	}
	limit, err := normalizeTaskPageLimit(filter.Limit)
	if err != nil {
		return storageport.TaskPage{}, err
	}
	if filter.Status != nil {
		if _, err := lifecycle.Restore("task-list-filter", *filter.Status); err != nil {
			return storageport.TaskPage{}, invalidArgument("unknown task status")
		}
	}
	if err := validateTaskTimeRange(filter.CreatedAfter, filter.CreatedBefore); err != nil {
		return storageport.TaskPage{}, err
	}

	clauses := make([]string, 0, 4)
	arguments := make([]any, 0, 8)
	if filter.Status != nil {
		clauses = append(clauses, "status = ?")
		arguments = append(arguments, *filter.Status)
	}
	if filter.CreatedAfter != nil {
		clauses = append(clauses, "created_at > ?")
		arguments = append(arguments, filter.CreatedAfter.UTC().UnixNano())
	}
	if filter.CreatedBefore != nil {
		clauses = append(clauses, "created_at < ?")
		arguments = append(arguments, filter.CreatedBefore.UTC().UnixNano())
	}
	if strings.TrimSpace(filter.PageToken) != "" {
		cursor, err := decodeTaskPageToken(filter.PageToken)
		if err != nil {
			return storageport.TaskPage{}, err
		}
		clauses = append(clauses, "(created_at < ? OR (created_at = ? AND task_id < ?))")
		arguments = append(arguments, cursor.CreatedAtNano, cursor.CreatedAtNano, cursor.TaskID)
	}

	query := `
SELECT task_id, intent, requirements_json, constraints_json, status,
       failure_code, failure_message, created_at, updated_at,
       started_at, completed_at, version
FROM tasks`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC, task_id DESC LIMIT ?"
	arguments = append(arguments, limit+1)

	rows, err := repository.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return storageport.TaskPage{}, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()
	records := make([]storageport.Task, 0, limit+1)
	for rows.Next() {
		record, err := scanTaskSummary(rows)
		if err != nil {
			return storageport.TaskPage{}, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return storageport.TaskPage{}, fmt.Errorf("read task list: %w", err)
	}
	if err := rows.Close(); err != nil {
		return storageport.TaskPage{}, fmt.Errorf("close task list: %w", err)
	}

	page = storageport.TaskPage{Tasks: records}
	if len(records) > limit {
		page.Tasks = records[:limit]
		last := page.Tasks[len(page.Tasks)-1]
		page.NextPageToken, err = encodeTaskPageToken(taskPageCursor{
			Version: taskPageTokenVersion, CreatedAtNano: last.CreatedAt.UnixNano(), TaskID: string(last.ID),
		})
		if err != nil {
			return storageport.TaskPage{}, err
		}
	}
	return page, nil
}

func normalizeTaskPageLimit(limit int) (int, error) {
	switch {
	case limit == 0:
		return storageport.DefaultTaskPageLimit, nil
	case limit < 0:
		return 0, invalidArgument("limit must not be negative")
	case limit > storageport.MaxTaskPageLimit:
		return 0, invalidArgument("limit exceeds maximum %d", storageport.MaxTaskPageLimit)
	default:
		return limit, nil
	}
}

func validateTaskTimeRange(after, before *time.Time) error {
	if after != nil && after.IsZero() {
		return invalidArgument("created_after must be a valid timestamp")
	}
	if before != nil && before.IsZero() {
		return invalidArgument("created_before must be a valid timestamp")
	}
	if after != nil && !time.Unix(0, after.UTC().UnixNano()).UTC().Equal(after.UTC()) {
		return invalidArgument("created_after is outside supported timestamp range")
	}
	if before != nil && !time.Unix(0, before.UTC().UnixNano()).UTC().Equal(before.UTC()) {
		return invalidArgument("created_before is outside supported timestamp range")
	}
	if after != nil && before != nil && after.After(*before) {
		return invalidArgument("created_after must not be later than created_before")
	}
	return nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanTaskSummary(scanner rowScanner) (storageport.Task, error) {
	var record storageport.Task
	var requirementsJSON, constraintsJSON, state string
	var failureCode, failureMessage sql.NullString
	var createdAt, updatedAt, startedAt, completedAt any
	if err := scanner.Scan(
		&record.ID, &record.Intent, &requirementsJSON, &constraintsJSON, &state,
		&failureCode, &failureMessage, &createdAt, &updatedAt,
		&startedAt, &completedAt, &record.Version,
	); err != nil {
		return storageport.Task{}, fmt.Errorf("scan task list item: %w", err)
	}
	record.State = lifecycle.State(state)
	record.FailureCode = failureCode.String
	record.FailureMessage = failureMessage.String
	if err := decodeJSON("task requirements", requirementsJSON, &record.Requirements); err != nil {
		return storageport.Task{}, fmt.Errorf("decode task %q summary: %w", record.ID, err)
	}
	if err := decodeJSON("task constraints", constraintsJSON, &record.Constraints); err != nil {
		return storageport.Task{}, fmt.Errorf("decode task %q summary: %w", record.ID, err)
	}
	var err error
	record.CreatedAt, err = decodeTime("task created_at", createdAt)
	if err != nil {
		return storageport.Task{}, fmt.Errorf("decode task %q summary: %w", record.ID, err)
	}
	record.UpdatedAt, err = decodeTime("task updated_at", updatedAt)
	if err != nil {
		return storageport.Task{}, fmt.Errorf("decode task %q summary: %w", record.ID, err)
	}
	record.StartedAt, err = decodeOptionalTime("task started_at", startedAt)
	if err != nil {
		return storageport.Task{}, fmt.Errorf("decode task %q summary: %w", record.ID, err)
	}
	record.CompletedAt, err = decodeOptionalTime("task completed_at", completedAt)
	if err != nil {
		return storageport.Task{}, fmt.Errorf("decode task %q summary: %w", record.ID, err)
	}
	if _, err := lifecycle.Restore(record.ID, record.State); err != nil || record.Version < 1 || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		return storageport.Task{}, invalidData("task %q summary", record.ID)
	}
	record.Steps = nil
	return record, nil
}

func encodeTaskPageToken(cursor taskPageCursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode task page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeTaskPageToken(token string) (taskPageCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return taskPageCursor{}, invalidArgument("invalid page token")
	}
	decoder := json.NewDecoder(strings.NewReader(string(decoded)))
	decoder.DisallowUnknownFields()
	var cursor taskPageCursor
	if err := decoder.Decode(&cursor); err != nil {
		return taskPageCursor{}, invalidArgument("invalid page token")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return taskPageCursor{}, invalidArgument("invalid page token")
	}
	if cursor.Version != taskPageTokenVersion || strings.TrimSpace(cursor.TaskID) == "" {
		return taskPageCursor{}, invalidArgument("unsupported or invalid page token")
	}
	return cursor, nil
}

func (repository *Repository) ListExecutionsByTask(ctx context.Context, taskID model.TaskID) (records []storageport.Execution, returnErr error) {
	defer func() { returnErr = classifyTaskQueryError(ctx, returnErr) }()
	if strings.TrimSpace(string(taskID)) == "" {
		return nil, invalidArgument("task ID is required")
	}
	if _, err := repository.database.SQLDB(); err != nil {
		return nil, err
	}
	transaction, err := repository.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin task %q execution query: %w", taskID, err)
	}
	defer transaction.Rollback()
	var exists int
	if err := transaction.QueryRowContext(ctx, "SELECT 1 FROM tasks WHERE task_id = ?", taskID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: task %q", storageport.ErrNotFound, taskID)
	} else if err != nil {
		return nil, fmt.Errorf("inspect task %q for executions: %w", taskID, err)
	}
	rows, err := transaction.QueryContext(ctx, `
SELECT executions.execution_id, executions.task_id, executions.step_id,
       executions.attempt_no, executions.node_id, executions.status,
       executions.request_json, executions.result_json, executions.failure_code,
       executions.failure_message, executions.started_at, executions.completed_at,
       executions.created_at, executions.updated_at, executions.version
FROM executions
JOIN task_steps ON task_steps.task_id = executions.task_id
               AND task_steps.step_id = executions.step_id
WHERE executions.task_id = ?
ORDER BY task_steps.sequence_no, executions.attempt_no, executions.execution_id`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list task %q executions: %w", taskID, err)
	}
	records = make([]storageport.Execution, 0)
	for rows.Next() {
		record, err := scanExecution(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan task %q execution: %w", taskID, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("read task %q executions: %w", taskID, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close task %q executions: %w", taskID, err)
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("commit task %q execution query: %w", taskID, err)
	}
	return records, nil
}
