ALTER TABLE "pool_registration"
    ADD COLUMN "leios_key_registration_age_unknown" BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE "pool_registration"
SET "leios_key_registration_age_unknown" = TRUE
WHERE ("certificate_id" IS NULL OR "certificate_id" = 0)
  AND "added_slot" > 0
  AND ("leios_key_public" IS NOT NULL OR "leios_key_possession_proof" IS NOT NULL);
