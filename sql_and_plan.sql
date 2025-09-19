SELECT
    s.sql_id,
    s.child_number,
    s.plan_hash_value,
    s.parsing_schema_name,
    s.module,
    s.action,
    s.cpu_time,
    s.elapsed_time,
    s.disk_reads,
    s.buffer_gets,
    s.rows_processed,
    s.executions,
    s.end_of_fetch_count,
    s.parse_calls,
    s.loads,
    s.invalidations,
    s.sharable_mem,
    s.persistent_mem,
    s.runtime_mem,
    s.sorts,
    s.fetches,
    s.command_type,
    s.optimizer_mode,
    s.optimizer_cost,
    s.optimized,
    s.is_bind_sensitive,
    s.is_bind_aware,
    s.is_shareable,
    s.is_obsolete,
    p.operation,
    p.options,
    p.object_owner,
    p.object_name,
    p.object_type,
    p.cardinality,
    p.bytes,
    p.cpu_cost,
    p.io_cost,
    p.access_predicates,
    p.filter_predicates
FROM
    v$sql s
JOIN
    v$sql_plan p
ON
    s.sql_id = p.sql_id
AND s.child_number = p.child_number;
