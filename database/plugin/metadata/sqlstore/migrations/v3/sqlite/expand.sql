-- Version 3 (v3alpha1) adds node_settings_gate, the storage backing the
-- persisted node settings gates enforced on startup by
-- database/nodesettings.Evaluate.
-- This file is immutable after release; changes are detected by its checksum.
CREATE TABLE IF NOT EXISTS `node_settings_gate` (`name` text NOT NULL,`value` text NOT NULL,`recorded_epoch` integer NOT NULL,`recorded_slot` integer NOT NULL,PRIMARY KEY (`name`));
