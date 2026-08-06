-- name: GetCommitTimestamp :one
SELECT timestamp
FROM commit_timestamp
WHERE id = 1;

-- name: SetCommitTimestamp :exec
INSERT INTO commit_timestamp (id, timestamp)
VALUES (1, $1)
ON CONFLICT (id) DO UPDATE SET timestamp = excluded.timestamp;

-- name: GetNodeSettings :one
SELECT storage_mode, network
FROM node_settings
WHERE id = 1;

-- name: InsertNodeSettings :execrows
INSERT INTO node_settings (id, storage_mode, network)
VALUES (1, $1, $2)
ON CONFLICT (id) DO NOTHING;

-- name: BackfillNodeSettingsNetwork :execrows
UPDATE node_settings
SET network = $1
WHERE id = 1 AND storage_mode = $2 AND network = '';
