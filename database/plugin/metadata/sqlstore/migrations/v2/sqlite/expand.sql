-- Version 2 (v2) adds node_settings_gate, the storage backing persisted
-- node settings gates enforced on startup by database/nodesettings.Evaluate.
CREATE TABLE IF NOT EXISTS `node_settings_gate` (`name` text NOT NULL,`value` text NOT NULL,`recorded_epoch` integer NOT NULL,`recorded_slot` integer NOT NULL,PRIMARY KEY (`name`));
