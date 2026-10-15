ALTER TABLE `pool_registration`
    ADD COLUMN `registration_index` BIGINT NOT NULL DEFAULT 0;
UPDATE `pool_registration`
SET `certificate_id` = 0
WHERE `certificate_id` IS NULL;
CREATE UNIQUE INDEX `idx_pool_reg_pool_slot_cert_index`
    ON `pool_registration`
       (`pool_id`, `added_slot`, `certificate_id`, `registration_index`);
DROP INDEX `idx_pool_reg_pool_slot` ON `pool_registration`;
