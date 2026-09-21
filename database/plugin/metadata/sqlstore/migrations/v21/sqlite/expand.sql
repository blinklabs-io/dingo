-- Records the balance ReconcileAccountRewardBalance overwrote to, separately
-- from previous_reward (the balance it overwrote away from). previous_reward
-- alone is enough for DeleteAccountRewardsAfterSlot's withdrawal-restore
-- branch, which only ever needs to invert the overwrite, but
-- historicalRewardsBatch (dingo #4529) also walks this withdrawal-shaped row
-- to reconstruct balances at boundaries before the correction's slot, and
-- previous_reward there resolves to the pre-correction, wrong value. NULL
-- for every ordinary withdrawal row; only ReconcileAccountRewardBalance sets
-- it.
ALTER TABLE `account_reward_delta` ADD COLUMN `reconciled_amount` text;
