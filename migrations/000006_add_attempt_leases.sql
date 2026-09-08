BEGIN;

-- A lease fences state changes from stale coordinator replicas. Existing
-- active attempts are intentionally left unleased so the first coordinator
-- after migration can acquire them. Terminal attempts retain their last token
-- so a response-lost terminal transition can be replayed idempotently.
ALTER TABLE public.attempts
    ADD COLUMN lease_owner text,
    ADD COLUMN lease_token uuid,
    ADD COLUMN lease_expires_at timestamptz,
    ADD CONSTRAINT attempts_lease_consistent
        CHECK (
            (
                lease_owner IS NULL
                AND lease_token IS NULL
                AND lease_expires_at IS NULL
            )
            OR (
                lease_owner = btrim(lease_owner)
                AND octet_length(lease_owner) BETWEEN 1 AND 255
                AND lease_token IS NOT NULL
                AND lease_expires_at IS NOT NULL
            )
        );

CREATE INDEX attempts_active_lease_idx
    ON public.attempts (executor, lease_expires_at)
    WHERE state IN ('starting', 'running');

COMMIT;
