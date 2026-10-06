-- Closure effects belong to the certifying RB for rollback, but execute on
-- its parent's unticked ledger state for epoch snapshots and fee accounting.
CREATE TABLE IF NOT EXISTS `leios_transaction_context` (
    `transaction_id` integer PRIMARY KEY NOT NULL,
    `slot` integer NOT NULL,
    FOREIGN KEY (`transaction_id`) REFERENCES `transaction`(`id`) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS `idx_leios_transaction_context_slot`
    ON `leios_transaction_context`(`slot`);
