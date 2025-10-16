WITH hist AS (
  SELECT
      s.inst_id,
      TRUNC(s.last_active_time, 'HH24') AS sample_hour,
      /* Clamp per-row ratios to [0,1] before aggregating */
      AVG( LEAST(1, s.cpu_time / GREATEST(s.elapsed_time, 1)) ) AS avg_cpu_pct,
      MAX( LEAST(1, s.cpu_time / GREATEST(s.elapsed_time, 1)) ) AS max_cpu_pct,
      COUNT(*) AS query_count,
      AVG( (s.elapsed_time/1e6) / GREATEST(s.executions, 1) ) AS avg_elapsed_sec,
      SUM(s.disk_reads)   AS total_disk_reads,
      SUM(s.buffer_gets)  AS total_buffer_gets,
      AVG(s.executions)   AS avg_executions
  FROM gv$sql s
  WHERE s.last_active_time >= SYSDATE - 10
    AND s.elapsed_time > 0
  GROUP BY s.inst_id, TRUNC(s.last_active_time, 'HH24')
),
sys AS (
  SELECT
      m.inst_id,
      TRUNC(m.begin_time, 'HH24') AS sample_hour,
      /* Sysmetric is 0–100; convert to 0–1 and clamp */
      AVG( LEAST(1, GREATEST(0, m.value/100)) ) AS sys_cpu_pct
  FROM gv$sysmetric_history m
  WHERE m.metric_name = 'Host CPU Utilization (%)'
    AND m.begin_time >= SYSDATE - 10
  GROUP BY m.inst_id, TRUNC(m.begin_time, 'HH24')
)
SELECT
    h.inst_id,
    h.sample_hour,
    /* already 0–1 and clamped */
    h.avg_cpu_pct,
    h.max_cpu_pct,
    h.query_count,
    h.avg_elapsed_sec,
    h.total_disk_reads,
    h.total_buffer_gets,
    h.avg_executions,
    NVL(sys.sys_cpu_pct, 0) AS system_cpu_pct
FROM hist h
LEFT JOIN sys
  ON sys.inst_id = h.inst_id
 AND sys.sample_hour = h.sample_hour
ORDER BY h.sample_hour, h.inst_id;
