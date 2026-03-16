-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

CREATE INDEX IF NOT EXISTS flows_v3_idx_gin_tags_combined ON flows_v3 USING gin ((tags || system_tags));

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

DROP INDEX IF EXISTS flows_v3_idx_gin_tags_combined;

-- +goose StatementEnd
