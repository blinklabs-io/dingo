-- Locate the voting procedure behind each history entry, so every cast of a
-- vote can be listed with its transaction. Entries written before this
-- version leave both columns NULL.
ALTER TABLE `governance_vote_history` ADD COLUMN `tx_hash` blob;
ALTER TABLE `governance_vote_history` ADD COLUMN `cert_index` integer;
