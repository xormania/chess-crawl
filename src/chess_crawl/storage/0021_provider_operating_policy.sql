-- Operator-managed acquisition settings are independent of deployment defaults.
-- Updates retain an append-only history and use explicit expected versions.
CREATE TABLE provider_operating_policies (
    provider TEXT PRIMARY KEY REFERENCES providers(key),
    version BIGINT NOT NULL CHECK (version > 0),
    min_delay_s DOUBLE PRECISION NOT NULL CHECK (min_delay_s >= 0 AND min_delay_s < 'Infinity'::float8),
    max_retries BIGINT NOT NULL CHECK (max_retries >= 0),
    updated_at BIGINT NOT NULL
);

CREATE TABLE provider_operating_policy_history (
    provider TEXT NOT NULL REFERENCES providers(key),
    version BIGINT NOT NULL CHECK (version > 0),
    min_delay_s DOUBLE PRECISION NOT NULL CHECK (min_delay_s >= 0 AND min_delay_s < 'Infinity'::float8),
    max_retries BIGINT NOT NULL CHECK (max_retries >= 0),
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (provider, version)
);

CREATE FUNCTION preserve_operating_policy_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Provider operating policy history is append-only';
END;
$$;
CREATE TRIGGER operating_policy_history_is_append_only
    BEFORE UPDATE OR DELETE ON provider_operating_policy_history
    FOR EACH ROW EXECUTE FUNCTION preserve_operating_policy_history();
