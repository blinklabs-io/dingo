CREATE TABLE commit_timestamp (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT
);

CREATE TABLE node_settings (
    id BIGINT PRIMARY KEY,
    storage_mode VARCHAR(16) NOT NULL,
    network VARCHAR(64) NOT NULL
);

CREATE TABLE node_settings_gate (
    name           VARCHAR(64) PRIMARY KEY,
    value          VARCHAR(255) NOT NULL,
    recorded_epoch BIGINT NOT NULL,
    recorded_slot  BIGINT NOT NULL
);
