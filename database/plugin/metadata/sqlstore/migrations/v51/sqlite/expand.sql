-- Genesis pools are registered in epoch 0. Rows written before genesis
-- recorded that epoch carry NULL with a known age. The backfill recomputes
-- the registration epoch of every live keyed registration.
UPDATE `pool_registration`
SET `leios_key_registration_epoch` = 0
WHERE (`certificate_id` IS NULL OR `certificate_id` = 0)
  AND `added_slot` = 0
  AND `leios_key_public` IS NOT NULL
  AND `leios_key_possession_proof` IS NOT NULL
  AND `leios_key_registration_epoch` IS NULL
  AND `leios_key_registration_age_unknown` = FALSE;
