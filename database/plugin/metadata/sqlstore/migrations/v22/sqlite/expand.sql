CREATE TABLE IF NOT EXISTS drep_expiry_history (
    credential_tag INTEGER NOT NULL,
    credential BLOB NOT NULL,
    added_slot INTEGER NOT NULL,
    previous_expiry_epoch INTEGER NOT NULL,
    previous_last_activity_epoch INTEGER NOT NULL,
    PRIMARY KEY (credential_tag, credential, added_slot)
);

CREATE INDEX IF NOT EXISTS idx_drep_expiry_history_slot
    ON drep_expiry_history (added_slot);

CREATE TABLE IF NOT EXISTS drep_expiry_epoch_event (
    added_slot INTEGER PRIMARY KEY
);
