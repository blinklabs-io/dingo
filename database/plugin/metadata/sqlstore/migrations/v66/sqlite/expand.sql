-- One row per pool with certificate history, and the part of that history that
-- answers the epoch-effective registration query: every registration and
-- retirement at or after horizon_slot plus the newest of each kind below it.
-- Every write that adds or removes a pool certificate, or changes a key the
-- certificates are ranked by, recomputes the pool's rows from
-- pool_registration and pool_retirement in the same transaction; readers do
-- not check them against the history.
CREATE TABLE IF NOT EXISTS `pool_current_state` (
    `pool_id` integer PRIMARY KEY,
    `horizon_slot` integer NOT NULL DEFAULT 0,
    CONSTRAINT `fk_pool_current_state_pool` FOREIGN KEY (`pool_id`) REFERENCES `pool`(`id`) ON DELETE CASCADE
);
-- kind is 0 for a registration and 1 for a retirement; event_rank 0 is the
-- newest of its kind under the history ranking. effective_slot is the Leios
-- closure context slot of the certificate's transaction when there is one,
-- else added_slot. synthetic is 1 for a retirement without a certificate.
-- epoch is a retirement's target epoch.
CREATE TABLE IF NOT EXISTS `pool_current_event` (
    `pool_id` integer NOT NULL,
    `kind` integer NOT NULL,
    `event_rank` integer NOT NULL,
    `event_id` integer NOT NULL,
    `effective_slot` integer NOT NULL,
    `added_slot` integer NOT NULL,
    `synthetic` integer NOT NULL DEFAULT 0,
    `block_index` integer NOT NULL DEFAULT 0,
    `cert_index` integer NOT NULL DEFAULT 0,
    `registration_index` integer NOT NULL DEFAULT 0,
    `epoch` integer NOT NULL DEFAULT 0,
    PRIMARY KEY (`pool_id`, `kind`, `event_rank`),
    CONSTRAINT `fk_pool_current_event_pool` FOREIGN KEY (`pool_id`) REFERENCES `pool`(`id`) ON DELETE CASCADE
);
