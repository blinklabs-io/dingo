-- Run as a Postgres admin role against the Dingo metadata database.
-- Replace dingo_metadata, the Dingo owner role, and the initial password
-- before first running. Reruns refresh grants only; rotate an existing
-- password with an explicit ALTER ROLE.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dingo_gov_lens') THEN
    CREATE ROLE dingo_gov_lens LOGIN PASSWORD 'change-me';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE dingo_metadata TO dingo_gov_lens;
GRANT USAGE ON SCHEMA public TO dingo_gov_lens;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dingo_gov_lens;
ALTER DEFAULT PRIVILEGES FOR ROLE dingo IN SCHEMA public
  GRANT SELECT ON TABLES TO dingo_gov_lens;
