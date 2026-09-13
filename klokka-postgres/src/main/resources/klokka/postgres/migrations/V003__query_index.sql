-- Observation queries (countsByStatus, listJobs, getJob) filter on whole states: any
-- status, including terminal ones like Succeeded or Cancelled, is a legal dashboard
-- filter. That is unlike the claim path's partial indexes above, which only cover the
-- claimable subset (Scheduled/Enqueued, Failed, Running) and are silent on the rest.
-- This index is unconditional across all states and carries id DESC so it can serve
-- listJobs' "most recently persisted first" ordering directly.
CREATE INDEX klokka_jobs_query_idx ON klokka_jobs (state, id DESC);
