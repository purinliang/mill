-- Adds persisted resource classes and workload resource limits.
BEGIN;

-- Store resolved values with each job so retries preserve their original Pod
-- resources even if the server-defined class profiles change later.
ALTER TABLE public.jobs
    ADD COLUMN resource_class text NOT NULL DEFAULT 'small',
    ADD COLUMN workload_cpu_request_millis bigint NOT NULL DEFAULT 100,
    ADD COLUMN workload_cpu_limit_millis bigint NOT NULL DEFAULT 1000,
    ADD COLUMN workload_memory_request_bytes bigint NOT NULL DEFAULT 33554432,
    ADD COLUMN workload_memory_limit_bytes bigint NOT NULL DEFAULT 134217728,
    ADD CONSTRAINT jobs_resource_class_valid
        CHECK (resource_class IN ('small', 'medium', 'large')),
    ADD CONSTRAINT jobs_workload_cpu_valid
        CHECK (
            workload_cpu_request_millis > 0
            AND workload_cpu_limit_millis >= workload_cpu_request_millis
        ),
    ADD CONSTRAINT jobs_workload_memory_valid
        CHECK (
            workload_memory_request_bytes > 0
            AND workload_memory_limit_bytes >= workload_memory_request_bytes
        );

COMMIT;
