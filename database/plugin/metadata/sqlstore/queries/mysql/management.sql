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

-- name: GetNodeSettingsGates :many
SELECT name, value
FROM node_settings_gate;

-- name: UpsertNodeSettingsGate :exec
INSERT INTO node_settings_gate (name, value, recorded_epoch, recorded_slot)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    value = VALUES(value),
    recorded_epoch = VALUES(recorded_epoch),
    recorded_slot = VALUES(recorded_slot);

-- name: InsertNodeSettingsGateIfAbsent :execrows
-- INSERT IGNORE, not the ON DUPLICATE KEY UPDATE ... = VALUES(...) pattern
-- used above: that pattern always performs an UPDATE, whose RowsAffected is
-- ambiguous on a duplicate (0 normally, 1 under the driver's
-- CLIENT_FOUND_ROWS mode -- see SetNodeSettings's doc comment for the same
-- caveat). INSERT IGNORE either inserts or does nothing; RowsAffected is 1
-- or 0 with no such ambiguity, since no UPDATE ever runs.
INSERT IGNORE INTO node_settings_gate (name, value, recorded_epoch, recorded_slot)
VALUES (?, ?, ?, ?);
