-- Serve the epoch-wide epoch stakes count and page. `registered` was not in
-- any index, so the count read every stake row of the epoch. With the primary
-- key last, the count reads only matching entries and the page, which orders
-- by the primary key, needs no sort. The key holds no blob column: MySQL keys
-- those by prefix, which cannot serve an ORDER BY.
CREATE INDEX IF NOT EXISTS `idx_reward_stake_input_epoch_registered_id` ON `reward_stake_input`(`epoch`,`registered`,`id`);
