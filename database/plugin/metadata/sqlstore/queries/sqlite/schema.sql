CREATE TABLE commit_timestamp (
    id INTEGER PRIMARY KEY,
    timestamp INTEGER
);

CREATE TABLE node_settings (
    id INTEGER PRIMARY KEY,
    storage_mode TEXT NOT NULL,
    network TEXT NOT NULL
);

CREATE TABLE node_settings_gate (
    name           TEXT PRIMARY KEY,
    value          TEXT NOT NULL,
    recorded_epoch INTEGER NOT NULL,
    recorded_slot  INTEGER NOT NULL
);

CREATE TABLE tip (
    hash BLOB,
    id INTEGER PRIMARY KEY,
    slot INTEGER,
    block_number INTEGER
);

CREATE TABLE network_state (
    id INTEGER PRIMARY KEY,
    treasury TEXT NOT NULL,
    reserves TEXT NOT NULL,
    slot INTEGER NOT NULL UNIQUE
);

CREATE TABLE sync_state (
    sync_key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE epoch (
    nonce BLOB,
    evolving_nonce BLOB,
    candidate_nonce BLOB,
    last_epoch_block_nonce BLOB,
    id INTEGER PRIMARY KEY,
    epoch_id INTEGER UNIQUE,
    start_slot INTEGER,
    era_id INTEGER,
    slot_length INTEGER,
    length_in_slots INTEGER
);

CREATE TABLE block_nonce (
    hash BLOB,
    nonce BLOB,
    id INTEGER PRIMARY KEY,
    slot INTEGER,
    is_checkpoint BOOLEAN,
    UNIQUE (hash, slot)
);

CREATE TABLE datum (
    hash BLOB NOT NULL UNIQUE,
    raw_datum BLOB NOT NULL,
    id INTEGER PRIMARY KEY,
    added_slot INTEGER NOT NULL
);

CREATE TABLE script (
    hash BLOB UNIQUE,
    content BLOB,
    id INTEGER PRIMARY KEY,
    created_slot INTEGER,
    type INTEGER
);

CREATE TABLE pparams (
    cbor BLOB,
    id INTEGER PRIMARY KEY,
    added_slot INTEGER,
    epoch INTEGER,
    era_id INTEGER
);

CREATE TABLE pparam_update (
    genesis_hash BLOB,
    cbor BLOB,
    id INTEGER PRIMARY KEY,
    added_slot INTEGER,
    epoch INTEGER
);

CREATE TABLE network_donation (
    id INTEGER PRIMARY KEY,
    slot INTEGER NOT NULL UNIQUE,
    epoch INTEGER NOT NULL,
    amount INTEGER NOT NULL
);

CREATE TABLE import_checkpoint (
    id INTEGER PRIMARY KEY,
    import_key TEXT NOT NULL UNIQUE,
    phase TEXT NOT NULL
);

CREATE TABLE backfill_checkpoint (
    id INTEGER PRIMARY KEY,
    phase TEXT NOT NULL UNIQUE,
    last_slot INTEGER,
    total_slots INTEGER,
    started_at DATETIME,
    updated_at DATETIME,
    completed BOOLEAN
);

CREATE TABLE constitution (
    id INTEGER PRIMARY KEY,
    anchor_url TEXT NOT NULL,
    anchor_hash BLOB NOT NULL,
    policy_hash BLOB,
    added_slot INTEGER NOT NULL UNIQUE,
    deleted_slot INTEGER
);

CREATE TABLE committee_member (
    id INTEGER PRIMARY KEY,
    cold_cred_hash BLOB NOT NULL UNIQUE,
    expires_epoch INTEGER NOT NULL,
    added_slot INTEGER NOT NULL,
    deleted_slot INTEGER
);

CREATE TABLE committee_quorum (
    quorum TEXT,
    id INTEGER PRIMARY KEY,
    added_slot INTEGER NOT NULL UNIQUE
);

CREATE TABLE pool_stake_snapshot (
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    snapshot_type TEXT NOT NULL,
    pool_key_hash BLOB NOT NULL,
    total_stake TEXT NOT NULL,
    stake_denominator TEXT NOT NULL DEFAULT '0',
    delegator_count INTEGER NOT NULL,
    captured_slot INTEGER NOT NULL,
    calculation_version INTEGER NOT NULL DEFAULT 0,
    reward_account_auto_vote INTEGER NOT NULL DEFAULT 0,
    reward_account_auto_vote_resolved BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (epoch, snapshot_type, pool_key_hash)
);

CREATE TABLE epoch_summary (
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL UNIQUE,
    total_active_stake TEXT NOT NULL,
    total_pool_count INTEGER NOT NULL,
    total_delegators INTEGER NOT NULL,
    epoch_nonce BLOB,
    boundary_slot INTEGER NOT NULL,
    snapshot_ready BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE reward_ada_pots (
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL UNIQUE,
    treasury TEXT NOT NULL,
    reserves TEXT NOT NULL,
    fees TEXT NOT NULL,
    rewards TEXT NOT NULL,
    captured_slot INTEGER NOT NULL
);

CREATE TABLE reward_snapshot (
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    snapshot_type TEXT NOT NULL,
    total_active_stake TEXT NOT NULL,
    total_pool_count INTEGER NOT NULL,
    total_delegators INTEGER NOT NULL,
    captured_slot INTEGER NOT NULL,
    boundary_slot INTEGER NOT NULL,
    epoch_nonce BLOB,
    protocol_version INTEGER NOT NULL,
    authoritative BOOLEAN NOT NULL DEFAULT FALSE,
    calculation_version INTEGER NOT NULL DEFAULT 0,
    UNIQUE (epoch, snapshot_type)
);

CREATE TABLE reward_pool_input (
    margin TEXT,
    pool_key_hash BLOB NOT NULL,
    reward_account BLOB,
    blocks_produced INTEGER,
    total_blocks_in_epoch INTEGER,
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    pledge TEXT NOT NULL,
    delegated_stake TEXT NOT NULL,
    owner_stake TEXT NOT NULL DEFAULT '0',
    cost TEXT NOT NULL,
    delegator_count INTEGER NOT NULL,
    reward_account_credential_tag INTEGER NOT NULL DEFAULT 0,
    captured_slot INTEGER NOT NULL,
    boundary_slot INTEGER NOT NULL,
    UNIQUE (epoch, pool_key_hash)
);

CREATE TABLE reward_stake_input (
    pool_key_hash BLOB NOT NULL,
    staking_key BLOB NOT NULL,
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    credential_tag INTEGER NOT NULL DEFAULT 0,
    stake TEXT NOT NULL,
    owner BOOLEAN NOT NULL DEFAULT FALSE,
    registered BOOLEAN NOT NULL,
    captured_slot INTEGER NOT NULL,
    boundary_slot INTEGER NOT NULL,
    UNIQUE (epoch, pool_key_hash, credential_tag, staking_key)
);

CREATE TABLE reward_pool_output (
    apparent_performance TEXT,
    pool_key_hash BLOB NOT NULL,
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    optimal_reward TEXT NOT NULL,
    total_reward TEXT NOT NULL,
    leader_reward TEXT NOT NULL,
    member_reward_total TEXT NOT NULL,
    owner_stake TEXT NOT NULL,
    undistributed TEXT NOT NULL,
    unspendable TEXT NOT NULL,
    captured_slot INTEGER NOT NULL,
    boundary_slot INTEGER NOT NULL,
    UNIQUE (epoch, pool_key_hash)
);

CREATE TABLE reward_account_output (
    staking_key BLOB NOT NULL,
    pool_key_hash BLOB NOT NULL,
    reward_type TEXT NOT NULL,
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    credential_tag INTEGER NOT NULL DEFAULT 0,
    amount TEXT NOT NULL,
    spendable BOOLEAN NOT NULL,
    guarded BOOLEAN NOT NULL DEFAULT FALSE,
    captured_slot INTEGER NOT NULL,
    boundary_slot INTEGER NOT NULL,
    UNIQUE (
        epoch, credential_tag, staking_key, pool_key_hash, reward_type
    )
);

CREATE TABLE midnight_asset_creates (
    id INTEGER PRIMARY KEY,
    address BLOB NOT NULL,
    quantity INTEGER NOT NULL,
    tx_hash BLOB NOT NULL,
    output_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL,
    tx_index INTEGER NOT NULL,
    block_timestamp_ms INTEGER NOT NULL,
    UNIQUE (tx_hash, output_index)
);

CREATE TABLE midnight_asset_spends (
    id INTEGER PRIMARY KEY,
    address BLOB NOT NULL,
    quantity INTEGER NOT NULL,
    spending_tx_hash BLOB NOT NULL,
    utxo_tx_hash BLOB NOT NULL,
    utxo_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL,
    tx_index INTEGER NOT NULL,
    block_timestamp_ms INTEGER NOT NULL,
    UNIQUE (utxo_tx_hash, utxo_index)
);

CREATE TABLE midnight_registrations (
    id INTEGER PRIMARY KEY,
    full_datum BLOB NOT NULL,
    tx_hash BLOB NOT NULL,
    output_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL,
    tx_index INTEGER NOT NULL,
    block_timestamp_ms INTEGER NOT NULL,
    UNIQUE (tx_hash, output_index)
);

CREATE TABLE midnight_deregistrations (
    id INTEGER PRIMARY KEY,
    full_datum BLOB NOT NULL,
    tx_hash BLOB NOT NULL,
    utxo_tx_hash BLOB NOT NULL,
    utxo_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL,
    tx_index INTEGER NOT NULL,
    block_timestamp_ms INTEGER NOT NULL,
    UNIQUE (utxo_tx_hash, utxo_index)
);

CREATE TABLE midnight_governance_datums (
    id INTEGER PRIMARY KEY,
    datum_type TEXT NOT NULL,
    tx_hash BLOB NOT NULL,
    output_index INTEGER NOT NULL,
    datum BLOB NOT NULL,
    block_number INTEGER NOT NULL,
    UNIQUE (datum_type, tx_hash, output_index)
);

CREATE TABLE midnight_ariadne_params (
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL UNIQUE,
    datum BLOB NOT NULL
);

CREATE TABLE midnight_ariadne_rollbacks (
    id INTEGER PRIMARY KEY,
    block_number INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    previous_exists BOOLEAN NOT NULL,
    previous_datum BLOB,
    UNIQUE (block_number, epoch)
);

CREATE TABLE midnight_epoch_candidates (
    id INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL UNIQUE,
    block_number INTEGER NOT NULL DEFAULT 0,
    candidates_cbor BLOB NOT NULL
);

CREATE TABLE midnight_committee_candidate_registrations (
    id INTEGER PRIMARY KEY,
    tx_hash BLOB NOT NULL,
    output_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    slot_number INTEGER NOT NULL,
    tx_index INTEGER NOT NULL,
    tx_inputs_cbor BLOB NOT NULL,
    UNIQUE (tx_hash, output_index)
);

CREATE TABLE offchain_metadata (
    fetched_at DATETIME,
    next_fetch_after DATETIME,
    created_at DATETIME,
    updated_at DATETIME,
    url TEXT NOT NULL,
    source_type TEXT NOT NULL,
    status TEXT NOT NULL,
    content_type TEXT,
    last_error TEXT,
    hash BLOB NOT NULL,
    body_hash BLOB,
    content BLOB,
    id INTEGER PRIMARY KEY,
    fetch_attempts INTEGER,
    last_http_status INTEGER,
    UNIQUE (source_type, url, hash)
);

CREATE TABLE utxo (
    transaction_id INTEGER,
    collateral_return_for_tx_id INTEGER UNIQUE,
    tx_id BLOB,
    payment_key BLOB,
    staking_key BLOB,
    credential_tag INTEGER NOT NULL DEFAULT 0,
    datum_hash BLOB,
    spent_at_tx_id BLOB,
    referenced_by_tx_id BLOB,
    collateral_by_tx_id BLOB,
    id INTEGER PRIMARY KEY,
    added_slot INTEGER,
    deleted_slot INTEGER,
    amount TEXT,
    output_idx INTEGER,
    payment_script BOOLEAN,
    UNIQUE (tx_id, output_idx)
);

CREATE TABLE asset (
    name BLOB,
    name_hex BLOB,
    policy_id BLOB,
    fingerprint BLOB,
    id INTEGER PRIMARY KEY,
    utxo_id INTEGER,
    amount TEXT,
    UNIQUE (name, policy_id, utxo_id)
);

CREATE TABLE asset_mint_burn (
    id INTEGER PRIMARY KEY,
    tx_hash BLOB,
    policy_id BLOB,
    name BLOB,
    fingerprint BLOB,
    slot INTEGER,
    quantity TEXT,
    tx_index INTEGER,
    UNIQUE (tx_hash, policy_id, name)
);

CREATE TABLE account (
    staking_key BLOB,
    credential_tag INTEGER NOT NULL DEFAULT 0,
    pool BLOB,
    drep BLOB,
    id INTEGER PRIMARY KEY,
    added_slot INTEGER,
    created_slot INTEGER NOT NULL DEFAULT 0,
    certificate_id INTEGER,
    reward TEXT,
    drep_type INTEGER DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    expiration_epoch INTEGER DEFAULT 0,
    UNIQUE (credential_tag, staking_key)
);

CREATE TABLE drep (
    anchor_url TEXT,
    credential BLOB,
    anchor_hash BLOB,
    id INTEGER PRIMARY KEY,
    added_slot INTEGER,
    credential_tag INTEGER NOT NULL DEFAULT 0,
    last_activity_epoch INTEGER DEFAULT 0,
    expiry_epoch INTEGER DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    UNIQUE (credential_tag, credential)
);

CREATE TABLE registration_drep (
    anchor_url TEXT,
    drep_credential BLOB,
    anchor_hash BLOB,
    certificate_id INTEGER,
    id INTEGER PRIMARY KEY,
    credential_tag INTEGER NOT NULL DEFAULT 0,
    added_slot INTEGER,
    deposit_amount TEXT,
    UNIQUE (credential_tag, drep_credential, added_slot)
);

CREATE TABLE "transaction" (
    hash BLOB,
    block_hash BLOB,
    metadata BLOB,
    slot INTEGER,
    type INTEGER,
    id INTEGER PRIMARY KEY,
    fee TEXT,
    collateral_fee TEXT,
    ttl TEXT,
    block_index INTEGER,
    valid BOOLEAN,
    UNIQUE (hash)
);

CREATE TABLE address_transaction (
    id INTEGER PRIMARY KEY,
    payment_key BLOB,
    staking_key BLOB,
    credential_tag INTEGER NOT NULL DEFAULT 0,
    transaction_id INTEGER,
    slot INTEGER,
    tx_index INTEGER
);

CREATE TABLE transaction_metadata_label (
    id INTEGER PRIMARY KEY,
    transaction_id INTEGER,
    label TEXT,
    slot INTEGER,
    cbor_value BLOB,
    json_value TEXT,
    UNIQUE (transaction_id, label)
);
