-- Preserve key/script credential tags for committee membership,
-- authorization, and resignation state. Existing rows are key credentials.
ALTER TABLE `committee_member`
    ADD COLUMN `cold_credential_tag` integer NOT NULL DEFAULT 0;
ALTER TABLE `committee_member`
    ADD COLUMN `term_start_slot` integer NOT NULL DEFAULT 0;
UPDATE `committee_member` SET `term_start_slot` = `added_slot`;
DROP INDEX IF EXISTS `idx_committee_member_cold_cred_hash`;
CREATE UNIQUE INDEX IF NOT EXISTS `idx_committee_member_cold_credential`
    ON `committee_member`(
        `cold_credential_tag`,`cold_cred_hash`,`added_slot`
    );

ALTER TABLE `auth_committee_hot`
    ADD COLUMN `cold_credential_tag` integer NOT NULL DEFAULT 0;
ALTER TABLE `auth_committee_hot`
    ADD COLUMN `hot_credential_tag` integer NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS `idx_auth_committee_hot_cold_credential_identity`
    ON `auth_committee_hot`(`cold_credential_tag`,`cold_credential`);
CREATE INDEX IF NOT EXISTS `idx_auth_committee_hot_hot_credential_identity`
    ON `auth_committee_hot`(`hot_credential_tag`,`host_credential`);

ALTER TABLE `resign_committee_cold`
    ADD COLUMN `cold_credential_tag` integer NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS `idx_resign_committee_cold_credential_identity`
    ON `resign_committee_cold`(`cold_credential_tag`,`cold_credential`);
