-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Step duration reads the newest non-null heartbeat across all tasks in a step.
-- The partial index both supplies that order and avoids indexing tasks that have
-- never emitted a heartbeat.
CREATE INDEX CONCURRENTLY IF NOT EXISTS tasks_v3_idx_flow_run_step_heartbeat_desc
ON tasks_v3 (flow_id, run_number, step_name, last_heartbeat_ts DESC)
WHERE last_heartbeat_ts IS NOT NULL;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX CONCURRENTLY IF EXISTS tasks_v3_idx_flow_run_step_heartbeat_desc;
-- +goose StatementEnd
