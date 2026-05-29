#!/usr/bin/env bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  -v app_password="${DINGO_GOV_LENS_PASSWORD:-dingo_gov_lens}" \
  -v postgres_db="${POSTGRES_DB}" \
  -v postgres_user="${POSTGRES_USER}" \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" <<'EOSQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dingo_gov_lens') THEN
    CREATE ROLE dingo_gov_lens LOGIN;
  END IF;
END
$$;

ALTER ROLE dingo_gov_lens LOGIN PASSWORD :'app_password';

GRANT CONNECT ON DATABASE :"postgres_db" TO dingo_gov_lens;
GRANT USAGE ON SCHEMA public TO dingo_gov_lens;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dingo_gov_lens;
ALTER DEFAULT PRIVILEGES FOR ROLE :"postgres_user" IN SCHEMA public
  GRANT SELECT ON TABLES TO dingo_gov_lens;
EOSQL
