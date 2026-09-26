UPDATE `pool_stake_snapshot`
SET `leios_key_registration_epoch` = (
    SELECT CASE
        WHEN registration.`leios_key_registration_epoch` IS NOT NULL
            THEN registration.`leios_key_registration_epoch`
        ELSE effective_epoch.`epoch_id` + 1
    END
    FROM `pool_registration` registration
    LEFT JOIN `epoch` effective_epoch
        ON effective_epoch.`id` = (
            SELECT `id` FROM `epoch`
            WHERE `start_slot` <= registration.`added_slot`
            ORDER BY `start_slot` DESC LIMIT 1
        )
    WHERE registration.`pool_key_hash` = `pool_stake_snapshot`.`pool_key_hash`
      AND registration.`leios_key_public` = `pool_stake_snapshot`.`leios_key_public`
      AND registration.`leios_key_possession_proof` = `pool_stake_snapshot`.`leios_key_possession_proof`
      AND (
          (registration.`leios_key_registration_epoch` IS NOT NULL
              AND registration.`leios_key_registration_epoch` <= `pool_stake_snapshot`.`epoch`)
          OR (registration.`leios_key_registration_epoch` IS NULL
              AND registration.`leios_key_registration_age_unknown` = FALSE
              AND registration.`added_slot` <= `pool_stake_snapshot`.`captured_slot`
              AND effective_epoch.`epoch_id` + 1 <= `pool_stake_snapshot`.`epoch`)
      )
    ORDER BY CASE
        WHEN registration.`leios_key_registration_epoch` IS NOT NULL
            THEN registration.`leios_key_registration_epoch`
        ELSE effective_epoch.`epoch_id` + 1
    END DESC, registration.`added_slot` DESC, registration.`id` DESC
    LIMIT 1
)
WHERE `leios_key_registration_epoch` IS NULL
  AND `leios_key_public` IS NOT NULL
  AND `leios_key_possession_proof` IS NOT NULL;
