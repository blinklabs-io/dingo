-- Shift legacy nonnegative counters into the signed ordering used by the
-- application. Subtracting 2^63 preserves uint64 ordering in an int64 column.
UPDATE `pool_opcert_sequence`
SET `sequence` = `sequence` + (-9223372036854775807 - 1)
WHERE `sequence` >= 0;

UPDATE `pool`
SET `latest_op_cert_sequence` =
    `latest_op_cert_sequence` + (-9223372036854775807 - 1)
WHERE `latest_op_cert_sequence` >= 0;
