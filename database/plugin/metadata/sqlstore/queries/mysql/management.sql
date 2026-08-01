-- name: GetCommitTimestamp :one
SELECT timestamp
FROM commit_timestamp
WHERE id = 1;

-- name: SetCommitTimestamp :exec
INSERT INTO commit_timestamp (id, timestamp)
VALUES (1, ?)
ON DUPLICATE KEY UPDATE timestamp = VALUES(timestamp);

-- name: GetNodeSettings :one
SELECT storage_mode, network
FROM node_settings
WHERE id = 1;

-- name: InsertNodeSettings :execrows
INSERT INTO node_settings (id, storage_mode, network)
VALUES (1, ?, ?)
ON DUPLICATE KEY UPDATE id = VALUES(id);

-- name: BackfillNodeSettingsNetwork :execrows
UPDATE node_settings
SET network = ?
WHERE id = 1 AND storage_mode = ? AND network = '';
