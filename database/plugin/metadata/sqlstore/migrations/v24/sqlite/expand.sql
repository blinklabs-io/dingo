-- The fee pot a Mithril bootstrap collected for this row's own epoch up to
-- and including its anchor block (UTxOState.utxosFees minus SnapShots.ssFee
-- at import time). A NULL value means no epoch was imported into this row,
-- so the pre-fix whole-epoch local sum still applies to it (dingo #3975).
ALTER TABLE `reward_ada_pots` ADD COLUMN `imported_epoch_fees` text;
