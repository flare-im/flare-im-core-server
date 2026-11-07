-- New isolated cluster only. Passwords are supplied privately by the installer.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname IN('core_initialize','core_runtime')) THEN
   RAISE EXCEPTION 'Core initial roles already exist; do not adopt an existing cluster';
 END IF;
END $$;
CREATE ROLE core_initialize LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE core_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
