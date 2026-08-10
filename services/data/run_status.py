import os

# Time before a run with a heartbeat is considered inactive (and thus failed). Default 6
# minutes (in seconds). Single source of truth for the derived run status, shared by the
# metadata service and the ui_backend so both agree on what the UI reports.
RUN_INACTIVE_CUTOFF_TIME = int(os.environ.get("RUN_INACTIVE_CUTOFF_TIME", 60 * 6))


def run_status_joins(table_name, metadata_table, task_table):
    # The LATERAL joins pull the 'end' step's attempt metadata and the newest task
    # heartbeat that the run status derives from.
    # Returned as a list of SQL fragments to splice into a run query's FROM clause.
    return [
        """
        LEFT JOIN LATERAL (
            SELECT
                ts_epoch,
                (CASE
                    WHEN pg_typeof(value)='jsonb'::regtype
                    THEN value::jsonb->>0
                    ELSE value::text
                END)::boolean as value
            FROM {metadata_table} as attempt_ok
            WHERE
                {table_name}.flow_id = attempt_ok.flow_id AND
                {table_name}.run_number = attempt_ok.run_number AND
                attempt_ok.step_name = 'end' AND
                attempt_ok.field_name = 'attempt_ok'
            ORDER BY ts_epoch DESC
            LIMIT 1
        ) as end_attempt_ok ON true
        """.format(table_name=table_name, metadata_table=metadata_table),
        """
        LEFT JOIN LATERAL (
            SELECT ts_epoch
            FROM {metadata_table} as attempt
            WHERE
                {table_name}.flow_id = attempt.flow_id AND
                {table_name}.run_number = attempt.run_number AND
                attempt.step_name = 'end' AND
                attempt.field_name = 'attempt' AND
                end_attempt_ok.value IS FALSE
            ORDER BY ts_epoch DESC
            LIMIT 1
        ) as end_attempt ON true
        """.format(table_name=table_name, metadata_table=metadata_table),
        """
        LEFT JOIN LATERAL (
            SELECT last_heartbeat_ts
            FROM {task_table} AS task_heartbeat
            WHERE
                task_heartbeat.flow_id = {table_name}.flow_id AND
                task_heartbeat.run_number = {table_name}.run_number AND
                task_heartbeat.last_heartbeat_ts IS NOT NULL
            ORDER BY task_heartbeat.last_heartbeat_ts DESC
            LIMIT 1
        ) as latest_task_heartbeat ON true
        """.format(table_name=table_name, task_table=task_table),
    ]


def run_status_case(table_name, cutoff=RUN_INACTIVE_CUTOFF_TIME):
    # Derived run status (running/completed/failed). Depends on run_status_joins being
    # present. A run is 'running' while the end step is retrying, its own heartbeat is
    # fresh, or any task heartbeat is fresh. A final end-step result still wins.
    return """
        (CASE
            WHEN end_attempt IS NOT NULL
                AND end_attempt_ok.ts_epoch < end_attempt.ts_epoch
            THEN 'running'
            WHEN end_attempt_ok IS NOT NULL AND end_attempt_ok.value IS TRUE
            THEN 'completed'
            WHEN end_attempt_ok IS NOT NULL AND end_attempt_ok.value IS FALSE
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)<={cutoff}
            THEN 'running'
            WHEN latest_task_heartbeat.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-latest_task_heartbeat.last_heartbeat_ts)<={cutoff}
            THEN 'running'
            ELSE 'failed'
        END) AS status
        """.format(table_name=table_name, cutoff=cutoff)
