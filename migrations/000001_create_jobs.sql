BEGIN;

CREATE TABLE public.jobs (
    id uuid PRIMARY KEY DEFAULT uuidv7(),

    idempotency_key text NOT NULL,
    workload_image_ref text NOT NULL,
    workload_args text[] NOT NULL DEFAULT '{}'::text[],
    dataset_manifest_uri text NOT NULL,
    output_uri text NOT NULL,

    state text NOT NULL DEFAULT 'preparing',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT jobs_idempotency_key_unique
        UNIQUE (idempotency_key),
    CONSTRAINT jobs_output_uri_unique
        UNIQUE (output_uri),
    CONSTRAINT jobs_idempotency_key_valid
        CHECK (
            idempotency_key = btrim(idempotency_key)
            AND octet_length(idempotency_key) BETWEEN 1 AND 255
        ),
    CONSTRAINT jobs_workload_image_ref_not_blank
        CHECK (btrim(workload_image_ref) <> ''),
    CONSTRAINT jobs_dataset_manifest_uri_not_blank
        CHECK (btrim(dataset_manifest_uri) <> ''),
    CONSTRAINT jobs_output_uri_not_blank
        CHECK (btrim(output_uri) <> ''),
    CONSTRAINT jobs_workload_args_no_nulls
        CHECK (array_position(workload_args, NULL) IS NULL),
    CONSTRAINT jobs_state_valid
        CHECK (state IN ('preparing', 'running', 'completed', 'failed'))
);

COMMIT;
