-- A credited reward round's spendable, unguarded reward_account_output rows
-- are part of their account's balance until folded into account.reward, which
-- happens where a stored balance must change (a withdrawal). folded marks a
-- row already added, so balance reads exclude it.
ALTER TABLE `reward_account_output` ADD COLUMN `folded` boolean NOT NULL DEFAULT false;
