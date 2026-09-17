-- Governance actions may omit their anchor hash and URL. Rebuild the two
-- SQLite tables that reference governance_proposal so the proposal columns
-- become nullable without changing the persisted proposal identity.
ALTER TABLE `governance_proposal` RENAME TO `governance_proposal_v15`;
ALTER TABLE `governance_proposal_ratification_history` RENAME TO `governance_proposal_ratification_history_v15`;
CREATE TABLE `governance_proposal` (`id` integer PRIMARY KEY AUTOINCREMENT,`tx_hash` blob NOT NULL,`action_index` integer NOT NULL,`action_type` integer NOT NULL,`proposed_epoch` integer NOT NULL,`expires_epoch` integer NOT NULL,`parent_tx_hash` blob,`parent_action_idx` integer,`enacted_epoch` integer,`enacted_slot` integer,`ratified_epoch` integer,`ratified_slot` integer,`policy_hash` blob,`anchor_url` text,`anchor_hash` blob,`deposit` integer NOT NULL,`return_address` blob,`gov_action_cbor` blob,`expired_epoch` integer,`expired_slot` integer,`added_slot` integer NOT NULL,`deleted_slot` integer);
INSERT INTO `governance_proposal` SELECT * FROM `governance_proposal_v15`;
CREATE TABLE `governance_proposal_ratification_history` (`id` integer PRIMARY KEY AUTOINCREMENT,`proposal_id` integer NOT NULL,`transition_slot` integer NOT NULL,`ratified_epoch` integer,`ratified_slot` integer,CONSTRAINT `fk_governance_proposal_ratification_history_proposal` FOREIGN KEY (`proposal_id`) REFERENCES `governance_proposal`(`id`) ON DELETE CASCADE);
INSERT INTO `governance_proposal_ratification_history` SELECT * FROM `governance_proposal_ratification_history_v15`;
DROP TABLE `governance_proposal_ratification_history_v15`;
DROP TABLE `governance_proposal_v15`;
CREATE INDEX `idx_governance_proposal_deleted_slot` ON `governance_proposal`(`deleted_slot`);
CREATE INDEX `idx_governance_proposal_added_slot` ON `governance_proposal`(`added_slot`);
CREATE INDEX `idx_governance_proposal_expired_slot` ON `governance_proposal`(`expired_slot`);
CREATE INDEX `idx_governance_proposal_expired_epoch` ON `governance_proposal`(`expired_epoch`);
CREATE INDEX `idx_governance_proposal_ratified_slot` ON `governance_proposal`(`ratified_slot`);
CREATE INDEX `idx_governance_proposal_enacted_slot` ON `governance_proposal`(`enacted_slot`);
CREATE INDEX `idx_gov_proposal_parent` ON `governance_proposal`(`parent_tx_hash`,`parent_action_idx`);
CREATE INDEX `idx_governance_proposal_expires_epoch` ON `governance_proposal`(`expires_epoch`);
CREATE INDEX `idx_governance_proposal_proposed_epoch` ON `governance_proposal`(`proposed_epoch`);
CREATE INDEX `idx_governance_proposal_action_type` ON `governance_proposal`(`action_type`);
CREATE UNIQUE INDEX `idx_proposal_tx_action` ON `governance_proposal`(`tx_hash`,`action_index`);
CREATE INDEX `idx_governance_proposal_ratification_history_transition` ON `governance_proposal_ratification_history`(`transition_slot`);
CREATE INDEX `idx_governance_proposal_ratification_history_proposal_transition` ON `governance_proposal_ratification_history`(`proposal_id`,`transition_slot`,`id`);
