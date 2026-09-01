-- The pool deposit a registration retains, as distinct from `deposit_amount`,
-- which is what the block era's certificate deposit function computed from the
-- protocol parameters in force at that registration's slot. The two differ for
-- a re-registration: cardano-ledger's POOL rule charges a deposit only when the
-- pool is not already registered, so a re-registration leaves `psDeposits`
-- alone and the pool keeps holding the deposit its first registration paid.
-- POOLREAP refunds `psDeposits`, so refunding `deposit_amount` created or
-- destroyed ledger value whenever a poolDeposit parameter change landed between
-- a pool's first and last registration.
ALTER TABLE `pool_registration` ADD COLUMN `deposit_held` text;
-- Backfill for a database written before this column existed. A legacy
-- registration is credited with its own `deposit_amount`, which is exactly the
-- value the pre-change refund path read from the latest registration, so the
-- migration reproduces the refund the node would already have applied and
-- neither creates nor destroys value on existing data. Carry-forward semantics
-- apply from this migration forward; a network that changed poolDeposit before
-- the upgrade must resync from genesis for byte-exact agreement with a
-- genesis-synced node. `COALESCE` covers legacy rows written with a NULL
-- deposit (genesis and Mithril-import registrations), which hold nothing.
-- Restricting the write to NULL keeps the statement re-runnable after an
-- interrupted upgrade without overwriting a carried-forward value.
UPDATE `pool_registration`
SET `deposit_held` = COALESCE(`deposit_amount`, '0')
WHERE `deposit_held` IS NULL;
