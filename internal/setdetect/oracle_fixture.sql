-- Objects the hand-labeled corpus cases use. Run once per oracle database.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE ROLE app_reader NOLOGIN;
CREATE ROLE app_writer NOLOGIN;
CREATE SCHEMA tenant_1;
CREATE SCHEMA tenant_2;
CREATE SCHEMA "Tenant One";

CREATE TABLE users (
    id bigint PRIMARY KEY, email text, name text, status text DEFAULT 'active',
    settings jsonb DEFAULT '{}', reset_at timestamptz, created_at timestamptz DEFAULT now(), tenant_id int
);
CREATE TABLE orders (
    id bigserial PRIMARY KEY, user_id bigint, total numeric, status text,
    created_at timestamptz DEFAULT now(), updated_at timestamptz, offset_ms int DEFAULT 0
);
CREATE TABLE datasets (id bigint PRIMARY KEY, name text, settings jsonb DEFAULT '{}');
CREATE TABLE assets (asset_id bigserial PRIMARY KEY, dataset_id bigint, charset text, name text, reset_at timestamptz);
CREATE TABLE app_settings (key text PRIMARY KEY, value text);
CREATE TABLE accounts (id bigint PRIMARY KEY, balance numeric);
CREATE TABLE audit_log (id bigserial PRIMARY KEY, msg text);
CREATE TABLE events (id bigserial PRIMARY KEY, payload jsonb, ts timestamptz DEFAULT now());
CREATE TABLE scratch_t (id int, v int);
CREATE TABLE scratch_move (id int);
CREATE INDEX scratch_idx ON scratch_t (id);
CREATE VIEW scratch_v AS SELECT id FROM scratch_t;

INSERT INTO users (id, email, name, settings) SELECT g, 'u' || g || '@example.com', 'user ' || g, '{"theme": "light"}' FROM generate_series(1, 1000) g;
INSERT INTO orders (user_id, total, status) SELECT 1 + g % 1000, g, 'open' FROM generate_series(1, 2000) g;
INSERT INTO datasets SELECT g, 'ds ' || g, '{"visibility": "public"}' FROM generate_series(1, 50) g;
INSERT INTO assets (dataset_id, charset, name) SELECT 1 + g % 50, 'utf8', 'a' || g FROM generate_series(1, 200) g;
INSERT INTO accounts SELECT g, 100 FROM generate_series(1, 10) g;
INSERT INTO app_settings VALUES ('theme', 'light');

-- Server-side setters (P9) and their clean counterparts.
CREATE FUNCTION f_tenant(t text) RETURNS text LANGUAGE sql AS $$ SELECT set_config('app.tenant_id', t, false) $$;
CREATE FUNCTION set_tenant(t text) RETURNS text LANGUAGE sql AS $$ SELECT set_config('app.tenant_id', t, false) $$;
CREATE FUNCTION f_tenant_rows(t text) RETURNS SETOF text LANGUAGE sql AS $$ SELECT set_config('app.tenant_id', t, false) $$;
CREATE FUNCTION f_tz() RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('TimeZone', 'Asia/Kolkata', false); END $$;
CREATE FUNCTION f_path() RETURNS void LANGUAGE plpgsql AS $$ BEGIN SET search_path = tenant_2, public; END $$;
CREATE FUNCTION f_timeout() RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('statement_timeout', '42s', false); END $$;
CREATE FUNCTION f_role() RETURNS void LANGUAGE plpgsql AS $$ BEGIN SET ROLE app_reader; END $$;
CREATE FUNCTION f_local(t text) RETURNS text LANGUAGE sql AS $$ SELECT set_config('app.tenant_id', t, true) $$;
CREATE FUNCTION f_setclause() RETURNS text LANGUAGE sql SET search_path = tenant_1 AS $$ SELECT current_setting('search_path') $$;
CREATE FUNCTION f_noop() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE PROCEDURE proc_set_tenant(t text) LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('app.tenant_id', t, false); END $$;
CREATE PROCEDURE proc_tenant(t text) LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('app.tenant_id', t, false); END $$;
CREATE PROCEDURE proc_path(s text) LANGUAGE plpgsql AS $$ BEGIN EXECUTE format('SET search_path = %I', s); END $$;
CREATE PROCEDURE proc_noop() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;
CREATE FUNCTION audit_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM set_config('app.last_audit', NEW.msg, false);
    RETURN NEW;
END $$;
CREATE TRIGGER audit_sets AFTER INSERT ON audit_log FOR EACH ROW WHEN (NEW.msg LIKE 'trigger%') EXECUTE FUNCTION audit_trigger();

GRANT ALL ON ALL TABLES IN SCHEMA public TO app_reader, app_writer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO app_reader, app_writer;
