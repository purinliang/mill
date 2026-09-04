BEGIN;

ALTER TABLE public.jobs
    ADD COLUMN dataset_manifest_sha256 text,
    ADD COLUMN task_count integer;

ALTER TABLE public.jobs
    ADD CONSTRAINT jobs_dataset_manifest_sha256_valid
        CHECK (
            dataset_manifest_sha256 IS NULL
            OR dataset_manifest_sha256 ~ '^[0-9a-f]{64}$'
        ),
    ADD CONSTRAINT jobs_task_count_valid
        CHECK (task_count IS NULL OR task_count > 0),
    ADD CONSTRAINT jobs_materialization_consistent
        CHECK (
            (
                dataset_manifest_sha256 IS NULL
                AND task_count IS NULL
                AND state IN ('preparing', 'failed')
            )
            OR (
                dataset_manifest_sha256 IS NOT NULL
                AND task_count IS NOT NULL
                AND state IN ('running', 'completed', 'failed')
            )
        );

CREATE TABLE public.tasks (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    job_id uuid NOT NULL REFERENCES public.jobs (id) ON DELETE CASCADE,
    shard_index integer NOT NULL,
    input_uri text NOT NULL,
    output_uri text NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT tasks_job_shard_unique
        UNIQUE (job_id, shard_index),
    CONSTRAINT tasks_output_uri_unique
        UNIQUE (output_uri),
    CONSTRAINT tasks_shard_index_valid
        CHECK (shard_index >= 0),
    CONSTRAINT tasks_input_uri_not_blank
        CHECK (btrim(input_uri) <> ''),
    CONSTRAINT tasks_output_uri_not_blank
        CHECK (btrim(output_uri) <> ''),
    CONSTRAINT tasks_state_valid
        CHECK (state IN ('pending', 'running', 'completed', 'failed'))
);

CREATE INDEX tasks_job_id_state_idx ON public.tasks (job_id, state);

COMMIT;
