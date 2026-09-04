BEGIN;

ALTER TABLE public.jobs
    RENAME COLUMN workload_image_ref TO executable_image_ref;
ALTER TABLE public.jobs
    RENAME COLUMN workload_args TO executable_args;
ALTER TABLE public.jobs
    RENAME COLUMN dataset_manifest_uri TO input_uri;
ALTER TABLE public.jobs
    RENAME COLUMN dataset_manifest_sha256 TO input_sha256;
ALTER TABLE public.jobs
    RENAME COLUMN output_uri TO output_root_uri;

ALTER TABLE public.jobs
    RENAME CONSTRAINT jobs_workload_image_ref_not_blank
        TO jobs_executable_image_ref_not_blank;
ALTER TABLE public.jobs
    RENAME CONSTRAINT jobs_workload_args_no_nulls
        TO jobs_executable_args_no_nulls;
ALTER TABLE public.jobs
    RENAME CONSTRAINT jobs_dataset_manifest_uri_not_blank
        TO jobs_input_uri_not_blank;
ALTER TABLE public.jobs
    RENAME CONSTRAINT jobs_dataset_manifest_sha256_valid
        TO jobs_input_sha256_valid;
ALTER TABLE public.jobs
    RENAME CONSTRAINT jobs_output_uri_unique
        TO jobs_output_root_uri_unique;
ALTER TABLE public.jobs
    RENAME CONSTRAINT jobs_output_uri_not_blank
        TO jobs_output_root_uri_not_blank;

ALTER TABLE public.jobs
    ADD COLUMN input_record_count bigint,
    ADD COLUMN parallelism integer;

UPDATE public.jobs
SET parallelism = 1;

ALTER TABLE public.jobs
    ALTER COLUMN parallelism SET NOT NULL,
    DROP CONSTRAINT jobs_materialization_consistent,
    ADD CONSTRAINT jobs_input_record_count_valid
        CHECK (input_record_count IS NULL OR input_record_count > 0),
    ADD CONSTRAINT jobs_parallelism_valid
        CHECK (parallelism BETWEEN 1 AND 10000),
    ADD CONSTRAINT jobs_materialization_consistent
        CHECK (
            (state = 'preparing' AND task_count IS NULL)
            OR state = 'failed'
            OR (
                state IN ('running', 'completed')
                AND input_sha256 IS NOT NULL
                AND task_count IS NOT NULL
            )
        );

ALTER TABLE public.tasks
    DROP CONSTRAINT tasks_output_uri_unique,
    DROP CONSTRAINT tasks_input_uri_not_blank,
    DROP CONSTRAINT tasks_output_uri_not_blank,
    DROP COLUMN input_uri,
    DROP COLUMN output_uri,
    ADD COLUMN input_start_byte bigint,
    ADD COLUMN input_end_byte bigint,
    ADD CONSTRAINT tasks_input_range_valid
        CHECK (
            (input_start_byte IS NULL AND input_end_byte IS NULL)
            OR (
                input_start_byte >= 0
                AND input_end_byte > input_start_byte
            )
        );

COMMIT;
