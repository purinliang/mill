BEGIN;

CREATE TABLE public.attempts (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    task_id uuid NOT NULL REFERENCES public.tasks (id) ON DELETE CASCADE,
    attempt_number integer NOT NULL,
    executor text NOT NULL,
    state text NOT NULL DEFAULT 'starting',
    external_id text,
    failure_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT attempts_task_number_unique
        UNIQUE (task_id, attempt_number),
    CONSTRAINT attempts_number_valid
        CHECK (attempt_number > 0),
    CONSTRAINT attempts_executor_valid
        CHECK (
            executor = btrim(executor)
            AND octet_length(executor) BETWEEN 1 AND 63
        ),
    CONSTRAINT attempts_state_valid
        CHECK (state IN ('starting', 'running', 'completed', 'failed')),
    CONSTRAINT attempts_external_id_valid
        CHECK (
            external_id IS NULL
            OR (
                btrim(external_id) <> ''
                AND octet_length(external_id) <= 255
            )
        ),
    CONSTRAINT attempts_failure_message_valid
        CHECK (
            failure_message IS NULL
            OR (
                btrim(failure_message) <> ''
                AND octet_length(failure_message) <= 4096
            )
        ),
    CONSTRAINT attempts_lifecycle_consistent
        CHECK (
            (
                state = 'starting'
                AND external_id IS NULL
                AND started_at IS NULL
                AND finished_at IS NULL
                AND failure_message IS NULL
            )
            OR (
                state = 'running'
                AND external_id IS NOT NULL
                AND started_at IS NOT NULL
                AND finished_at IS NULL
                AND failure_message IS NULL
            )
            OR (
                state = 'completed'
                AND external_id IS NOT NULL
                AND started_at IS NOT NULL
                AND finished_at IS NOT NULL
                AND failure_message IS NULL
            )
            OR (
                state = 'failed'
                AND finished_at IS NOT NULL
                AND failure_message IS NOT NULL
                AND (
                    (
                        external_id IS NULL
                        AND started_at IS NULL
                    )
                    OR (
                        external_id IS NOT NULL
                        AND started_at IS NOT NULL
                    )
                )
            )
        )
);

CREATE UNIQUE INDEX attempts_one_active_per_task_idx
    ON public.attempts (task_id)
    WHERE state IN ('starting', 'running');

CREATE INDEX attempts_task_id_state_idx
    ON public.attempts (task_id, state);

COMMIT;
