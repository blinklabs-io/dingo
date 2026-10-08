CREATE INDEX IF NOT EXISTS `idx_auth_committee_hot_cold_credential_prune_order`
    ON `auth_committee_hot`(
        `cold_credential_tag`,
        `cold_credential`,
        `added_slot` DESC,
        `certificate_id` DESC
    );
