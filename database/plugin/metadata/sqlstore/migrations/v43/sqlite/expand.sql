ALTER TABLE `pool_registration`
    ADD COLUMN `registration_index` integer NOT NULL DEFAULT 0;
UPDATE `pool_registration`
SET `certificate_id` = 0
WHERE `certificate_id` IS NULL;
DROP INDEX IF EXISTS `idx_pool_reg_pool_slot`;
CREATE UNIQUE INDEX IF NOT EXISTS `idx_pool_reg_pool_slot_cert_index`
    ON `pool_registration`
       (`pool_id`, `added_slot`, `certificate_id`, `registration_index`);
