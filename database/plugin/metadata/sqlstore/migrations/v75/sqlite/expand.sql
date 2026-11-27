-- The IndexerDB's CBOR entries for each output and transaction: a 52-byte
-- offset into a block, or the output's raw CBOR once its block has expired.
-- They are keyed by identity, as the rows they belong to are looked up.
CREATE TABLE IF NOT EXISTS `utxo_offset` (`tx_id` blob NOT NULL,`output_idx` integer NOT NULL,`data` blob NOT NULL,PRIMARY KEY (`tx_id`,`output_idx`)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS `transaction_offset` (`hash` blob NOT NULL,`data` blob NOT NULL,PRIMARY KEY (`hash`)) WITHOUT ROWID;
