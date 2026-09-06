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
