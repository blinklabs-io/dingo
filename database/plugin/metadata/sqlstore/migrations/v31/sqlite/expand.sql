-- Store applied reward rounds as indexed rows so pending reward lookups do
-- not need an epoch-sized SQL IN list.
CREATE TABLE IF NOT EXISTS `reward_credit_round` (
    `snapshot_epoch` integer PRIMARY KEY NOT NULL,
    `boundary_slot` integer NOT NULL
);

CREATE INDEX IF NOT EXISTS `idx_reward_credit_round_boundary_slot`
    ON `reward_credit_round`(`boundary_slot`);

CREATE INDEX IF NOT EXISTS `idx_reward_account_output_pending_round`
    ON `reward_account_output`(`spendable`, `guarded`, `folded`, `epoch`);
