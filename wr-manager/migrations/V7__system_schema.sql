-- Move all manager tables from public into a dedicated wr_system schema.
-- Guest DB roles (wr_ns_*) are never granted access to wr_system, so
-- WASM modules cannot read system tables (routing rules, secrets, etc.).

CREATE SCHEMA IF NOT EXISTS wr_system;

-- Move each table only if it currently lives in public.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'wr_migrations') THEN
    ALTER TABLE public.wr_migrations SET SCHEMA wr_system;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'wr_manager_lock') THEN
    ALTER TABLE public.wr_manager_lock SET SCHEMA wr_system;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'wr_engines') THEN
    ALTER TABLE public.wr_engines SET SCHEMA wr_system;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'wr_routing_rules') THEN
    ALTER TABLE public.wr_routing_rules SET SCHEMA wr_system;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'wr_schemas') THEN
    ALTER TABLE public.wr_schemas SET SCHEMA wr_system;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'wr_secrets') THEN
    ALTER TABLE public.wr_secrets SET SCHEMA wr_system;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'wr_managers') THEN
    ALTER TABLE public.wr_managers SET SCHEMA wr_system;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'wr_schedules') THEN
    ALTER TABLE public.wr_schedules SET SCHEMA wr_system;
  END IF;
END $$;
