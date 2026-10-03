-- Global edges retain their original discovery provenance; membership records
-- each run that actually processed the edge without duplicating the graph.
CREATE TABLE run_edges (
  crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id),
  discovery_edge_id BIGINT NOT NULL REFERENCES discovery_edges(id),
  PRIMARY KEY (crawl_run_id, discovery_edge_id)
);

-- Earlier schemas retained only one run per global edge. Preserve that
-- evidence without inferring unrecorded historical memberships.
INSERT INTO run_edges(crawl_run_id, discovery_edge_id)
SELECT crawl_run_id, id FROM discovery_edges WHERE crawl_run_id IS NOT NULL;
