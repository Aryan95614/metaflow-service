-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Run status finds the newest non-null task heartbeat for each listed run.
CREATE INDEX CONCURRENTLY IF NOT EXISTS tasks_v3_idx_flow_run_heartbeat_desc
  ON tasks_v3 (flow_id, run_number, last_heartbeat_ts DESC)
  WHERE last_heartbeat_ts IS NOT NULL;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS tasks_v3_idx_flow_run_heartbeat_desc;
-- +goose StatementEnd
