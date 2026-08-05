CREATE TABLE commit_timestamp (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT
);

CREATE TABLE node_settings (
    id BIGINT PRIMARY KEY,
    storage_mode VARCHAR(16) NOT NULL,
    network VARCHAR(64) NOT NULL
);
