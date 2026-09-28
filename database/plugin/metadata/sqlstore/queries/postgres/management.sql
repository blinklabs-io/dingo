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

-- name: GetNodeSettingsGates :many
SELECT name, value
FROM node_settings_gate;

-- name: UpsertNodeSettingsGate :exec
INSERT INTO node_settings_gate (name, value, recorded_epoch, recorded_slot)
VALUES ($1, $2, $3, $4)
ON CONFLICT (name) DO UPDATE SET
    value = excluded.value,
    recorded_epoch = excluded.recorded_epoch,
    recorded_slot = excluded.recorded_slot;

-- name: InsertNodeSettingsGateIfAbsent :execrows
INSERT INTO node_settings_gate (name, value, recorded_epoch, recorded_slot)
VALUES ($1, $2, $3, $4)
ON CONFLICT (name) DO NOTHING;
