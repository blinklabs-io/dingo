-- Distinguish an explicit slot-zero committee term start from an unset value.
ALTER TABLE `committee_member`
    ADD COLUMN `term_start_slot_set` boolean NOT NULL DEFAULT false;
