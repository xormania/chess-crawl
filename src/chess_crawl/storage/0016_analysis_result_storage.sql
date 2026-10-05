-- Count logical UTF-8 payload bytes, independently of JSONB/TOAST compression.
-- Existing results are accounted for without deleting or changing their content.
CREATE FUNCTION analysis_result_bytes(settings JSONB, output JSONB, implementation TEXT,
                                     implementation_version TEXT, workspace_id TEXT)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT octet_length(settings::text)::bigint + octet_length(output::text)
       + octet_length(implementation) + octet_length(implementation_version)
       + octet_length(workspace_id) + 128; -- Two SHA-256 signatures.
$$;

ALTER TABLE analysis_results ADD COLUMN stored_bytes BIGINT GENERATED ALWAYS AS (
  analysis_result_bytes(settings, output, implementation, implementation_version, workspace_id)
) STORED;

CREATE INDEX ix_results_retention ON analysis_results(workspace_id, created_at, id)
  INCLUDE(stored_bytes);
