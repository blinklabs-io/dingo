-- A governance action's deposit must not be refunded in the same epoch the
-- action is detected as expired: cardano-ledger drops an expired action (and
-- returns its deposit) one full epoch after marking it expired, mirroring the
-- existing ratified-this-epoch/enacted-next-epoch delay. A companion table
-- (rather than columns added to governance_proposal) keeps this migration a
-- plain CREATE TABLE: governance_proposal was last rebuilt via rename+recreate
-- in v16 (governance-proposal-optional-anchor), whose `SELECT *` copy is not
-- safe to replay against a governance_proposal that has gained columns since
-- (dingo#4411).
CREATE TABLE IF NOT EXISTS `governance_proposal_drop` (
    `proposal_id` integer PRIMARY KEY,
    `dropped_epoch` integer,
    `dropped_slot` integer,
    FOREIGN KEY (`proposal_id`) REFERENCES `governance_proposal`(`id`) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS `idx_governance_proposal_drop_epoch` ON `governance_proposal_drop`(`dropped_epoch`);
CREATE INDEX IF NOT EXISTS `idx_governance_proposal_drop_slot` ON `governance_proposal_drop`(`dropped_slot`);
