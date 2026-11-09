CREATE TABLE IF NOT EXISTS "reward_credit" (
    "boundary_slot" BIGINT NOT NULL,
    "account_id" BIGINT NOT NULL,
    "output_id" BIGINT NOT NULL,
    "amount" TEXT NOT NULL,
    PRIMARY KEY ("boundary_slot", "account_id", "output_id")
);

CREATE TABLE IF NOT EXISTS "reward_credit_slot" (
    "boundary_slot" BIGINT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS "reward_source" (
    "id" BIGSERIAL PRIMARY KEY,
    "hash" BYTEA NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "idx_reward_source_hash"
    ON "reward_source"("hash");

CREATE TABLE IF NOT EXISTS "reward_event_credit" (
    "id" BIGSERIAL PRIMARY KEY,
    "source_id" BIGINT NOT NULL,
    "account_id" BIGINT NOT NULL,
    "slot" BIGINT NOT NULL,
    "amount" TEXT NOT NULL,
    "post_snapshot" BOOLEAN NOT NULL DEFAULT FALSE,
    "after_withdrawal" BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE UNIQUE INDEX IF NOT EXISTS "idx_reward_event_credit_source"
    ON "reward_event_credit"("source_id", "account_id", "slot");
CREATE INDEX IF NOT EXISTS "idx_reward_event_credit_slot"
    ON "reward_event_credit"("slot", "account_id");
CREATE INDEX IF NOT EXISTS "idx_reward_event_credit_account"
    ON "reward_event_credit"("account_id", "slot");

CREATE TABLE IF NOT EXISTS "reward_withdrawal" (
    "tx_id" BIGINT NOT NULL,
    "account_id" BIGINT NOT NULL,
    "slot" BIGINT NOT NULL,
    "amount" TEXT NOT NULL,
    "previous_reward" TEXT NOT NULL,
    PRIMARY KEY ("tx_id", "account_id")
);
CREATE INDEX IF NOT EXISTS "idx_reward_withdrawal_account_slot"
    ON "reward_withdrawal"("account_id", "slot");
CREATE INDEX IF NOT EXISTS "idx_reward_withdrawal_slot"
    ON "reward_withdrawal"("slot");
