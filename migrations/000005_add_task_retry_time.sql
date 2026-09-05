BEGIN;

-- A retry waits durably without occupying a running task slot. Existing tasks
-- are immediately eligible; terminal tasks and jobs are not resurrected.
ALTER TABLE public.tasks
    ADD COLUMN available_at timestamptz NOT NULL DEFAULT now();

COMMIT;
