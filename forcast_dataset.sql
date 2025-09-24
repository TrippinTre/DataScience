-- ===========================================
-- CPU Forecasting Dataset (last :days days)
-- One row per snapshot end_time (RAC-aggregated)
-- Requires: AWR access (DBA_HIST_* views)
-- ===========================================

VAR days NUMBER;
BEGIN :days := 4; END;
/

WITH base AS (
  SELECT s.dbid,
         s.instance_number,
         s.snap_id,
         s.begin_interval_time AS begin_time,
         s.end_interval_time   AS end_time
  FROM   dba_hist_snapshot s
  WHERE  s.end_interval_time >= SYSDATE - :days
),
-- Pull selected system metrics and pivot
metrics AS (
  SELECT b.dbid, b.instance_number, b.snap_id,
         MAX(CASE WHEN m.metric_name = 'Host CPU Utilization (%)'
                  THEN m.value END) AS cpu_util_pct,
         MAX(CASE WHEN m.metric_name = 'Average Active Sessions'
                  THEN m.value END) AS aas,
         MAX(CASE WHEN m.metric_name = 'User Transaction Per Sec'
                  THEN m.value END) AS txn_per_sec,
         MAX(CASE WHEN m.metric_name = 'Logical Reads Per Sec'
                  THEN m.value END) AS logical_reads_per_sec,
         MAX(CASE WHEN m.metric_name = 'Executions Per Sec'
                  THEN m.value END) AS execs_per_sec,
         MAX(CASE WHEN m.metric_name = 'Hard Parse Count Per Sec'
                  THEN m.value END) AS hard_parses_per_sec,
         MAX(CASE WHEN m.metric_name = 'Physical Reads Per Sec'
                  THEN m.value END) AS phys_reads_per_sec,
         MAX(CASE WHEN m.metric_name = 'Buffer Cache Hit Ratio'
                  THEN m.value END) AS buffer_cache_hit_ratio
  FROM   dba_hist_sysmetric_summary m
  JOIN   base b
    ON   b.snap_id = m.snap_id
   AND   b.instance_number = m.instance_number
   AND   b.dbid = m.dbid
  GROUP BY b.dbid, b.instance_number, b.snap_id
),
-- Wait-class time per interval (centiseconds); convert to % of total waits
waits_raw AS (
  SELECT b.dbid, b.instance_number, b.snap_id,
         SUM(CASE WHEN w.wait_class = 'User I/O'        THEN w.time_waited END) AS user_io_cs,
         SUM(CASE WHEN w.wait_class = 'System I/O'      THEN w.time_waited END) AS system_io_cs,
         SUM(CASE WHEN w.wait_class = 'Concurrency'     THEN w.time_waited END) AS concurrency_cs,
         SUM(CASE WHEN w.wait_class = 'Commit'          THEN w.time_waited END) AS commit_cs,
         SUM(CASE WHEN w.wait_class = 'Application'     THEN w.time_waited END) AS application_cs,
         SUM(CASE WHEN w.wait_class = 'Network'         THEN w.time_waited END) AS network_cs,
         SUM(CASE WHEN w.wait_class = 'Configuration'   THEN w.time_waited END) AS configuration_cs,
         SUM(CASE WHEN w.wait_class = 'Administrative'  THEN w.time_waited END) AS administrative_cs,
         SUM(CASE WHEN w.wait_class = 'Queueing'        THEN w.time_waited END) AS queueing_cs,
         SUM(CASE WHEN w.wait_class = 'Scheduler'       THEN w.time_waited END) AS scheduler_cs,
         SUM(CASE WHEN w.wait_class = 'Other'           THEN w.time_waited END) AS other_cs
  FROM   dba_hist_waitclassmetric_summary w
  JOIN   base b
    ON   b.snap_id = w.snap_id
   AND   b.instance_number = w.instance_number
   AND   b.dbid = w.dbid
  GROUP BY b.dbid, b.instance_number, b.snap_id
),
waits_pct AS (
  SELECT dbid, instance_number, snap_id,
         user_io_cs, system_io_cs, concurrency_cs, commit_cs, application_cs,
         network_cs, configuration_cs, administrative_cs, queueing_cs, scheduler_cs, other_cs,
         (NVL(user_io_cs,0) + NVL(system_io_cs,0) + NVL(concurrency_cs,0) + NVL(commit_cs,0) +
          NVL(application_cs,0) + NVL(network_cs,0) + NVL(configuration_cs,0) +
          NVL(administrative_cs,0) + NVL(queueing_cs,0) + NVL(scheduler_cs,0) + NVL(other_cs,0)) AS total_cs
  FROM waits_raw
),
-- Memory (PGA allocated at snapshot) - bytes -> MB
pga AS (
  SELECT b.dbid, b.instance_number, b.snap_id,
         MAX(CASE WHEN p.name = 'total PGA allocated' THEN p.value END) / (1024*1024) AS pga_allocated_mb
  FROM   dba_hist_pgastat p
  JOIN   base b
    ON   b.snap_id = p.snap_id
   AND   b.instance_number = p.instance_number
   AND   b.dbid = p.dbid
  GROUP BY b.dbid, b.instance_number, b.snap_id
),
-- OS stats (load & CPU core count)
os AS (
  SELECT b.dbid, b.instance_number, b.snap_id,
         MAX(CASE WHEN o.stat_name = 'LOAD'          THEN o.value END) AS os_load,
         MAX(CASE WHEN o.stat_name = 'NUM_CPU_CORES' THEN o.value END) AS cpu_cores
  FROM   dba_hist_osstat o
  JOIN   base b
    ON   b.snap_id = o.snap_id
   AND   b.instance_number = o.instance_number
   AND   b.dbid = o.dbid
  GROUP BY b.dbid, b.instance_number, b.snap_id
),
-- Join everything at (dbid, instance, snap)
inst_level AS (
  SELECT b.dbid, b.instance_number, b.snap_id, b.begin_time, b.end_time,
         m.cpu_util_pct, m.aas, m.txn_per_sec, m.logical_reads_per_sec, m.execs_per_sec,
         m.hard_parses_per_sec, m.phys_reads_per_sec, m.buffer_cache_hit_ratio,
         p.pga_allocated_mb,
         o.os_load, o.cpu_cores,
         -- Wait % (0-100), safe divide
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.user_io_cs,0)       / w.total_cs ELSE NULL END AS wait_user_io_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.system_io_cs,0)     / w.total_cs ELSE NULL END AS wait_system_io_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.concurrency_cs,0)   / w.total_cs ELSE NULL END AS wait_concurrency_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.commit_cs,0)        / w.total_cs ELSE NULL END AS wait_commit_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.application_cs,0)   / w.total_cs ELSE NULL END AS wait_application_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.network_cs,0)       / w.total_cs ELSE NULL END AS wait_network_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.configuration_cs,0) / w.total_cs ELSE NULL END AS wait_configuration_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.administrative_cs,0)/ w.total_cs ELSE NULL END AS wait_administrative_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.queueing_cs,0)      / w.total_cs ELSE NULL END AS wait_queueing_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.scheduler_cs,0)     / w.total_cs ELSE NULL END AS wait_scheduler_pct,
         CASE WHEN w.total_cs > 0 THEN 100 * NVL(w.other_cs,0)         / w.total_cs ELSE NULL END AS wait_other_pct
  FROM   base b
  LEFT JOIN metrics   m ON m.dbid=b.dbid AND m.instance_number=b.instance_number AND m.snap_id=b.snap_id
  LEFT JOIN waits_pct w ON w.dbid=b.dbid AND w.instance_number=b.instance_number AND w.snap_id=b.snap_id
  LEFT JOIN pga       p ON p.dbid=b.dbid AND p.instance_number=b.instance_number AND p.snap_id=b.snap_id
  LEFT JOIN os        o ON o.dbid=b.dbid AND o.instance_number=b.instance_number AND o.snap_id=b.snap_id
),
-- Aggregate across RAC instances to produce a single row per end_time
rac_agg AS (
  SELECT
    end_time,
    AVG(cpu_util_pct)                           AS cpu_util_pct,                 -- target y
    SUM(aas)                                    AS aas,
    SUM(txn_per_sec)                            AS txn_per_sec,
    SUM(logical_reads_per_sec)                  AS logical_reads_per_sec,
    SUM(execs_per_sec)                          AS execs_per_sec,
    SUM(hard_parses_per_sec)                    AS hard_parses_per_sec,
    SUM(phys_reads_per_sec)                     AS phys_reads_per_sec,
    AVG(buffer_cache_hit_ratio)                 AS buffer_cache_hit_ratio,
    SUM(pga_allocated_mb)                       AS pga_allocated_mb,             -- across instances
    -- Normalize OS load by core count (mean of per-inst normalization)
    AVG(CASE WHEN cpu_cores > 0 THEN os_load / cpu_cores END) AS os_load_per_core,
    -- Wait-class percentages: average of instance-level percentages
    AVG(wait_user_io_pct)        AS wait_user_io_pct,
    AVG(wait_system_io_pct)      AS wait_system_io_pct,
    AVG(wait_concurrency_pct)    AS wait_concurrency_pct,
    AVG(wait_commit_pct)         AS wait_commit_pct,
    AVG(wait_application_pct)    AS wait_application_pct,
    AVG(wait_network_pct)        AS wait_network_pct,
    AVG(wait_configuration_pct)  AS wait_configuration_pct,
    AVG(wait_administrative_pct) AS wait_administrative_pct,
    AVG(wait_queueing_pct)       AS wait_queueing_pct,
    AVG(wait_scheduler_pct)      AS wait_scheduler_pct,
    AVG(wait_other_pct)          AS wait_other_pct
  FROM inst_level
  GROUP BY end_time
)
SELECT
  end_time                                        AS ts,                 -- use this as the time index
  cpu_util_pct                                    AS y_cpu_util_pct,     -- TARGET
  -- Features
  aas, txn_per_sec, logical_reads_per_sec, execs_per_sec,
  hard_parses_per_sec, phys_reads_per_sec,
  buffer_cache_hit_ratio,
  pga_allocated_mb,
  os_load_per_core,
  wait_user_io_pct, wait_system_io_pct, wait_concurrency_pct, wait_commit_pct,
  wait_application_pct, wait_network_pct, wait_configuration_pct,
  wait_administrative_pct, wait_queueing_pct, wait_scheduler_pct, wait_other_pct
FROM rac_agg
ORDER BY ts;


🧪 How to preprocess these columns for an NN (sklearn)

Target

y_cpu_util_pct (0–100): use directly. You can also scale to [0,1] (divide by 100) if you prefer.

Numeric, rate-like (often skewed)

aas, txn_per_sec, logical_reads_per_sec, execs_per_sec,
hard_parses_per_sec, phys_reads_per_sec, pga_allocated_mb

Handle missing with median imputation.

Consider log1p transform if heavy-tailed.

Scale with StandardScaler (mean 0, std 1).

Percentages / ratios

buffer_cache_hit_ratio, all wait_*_pct

They’re 0–100. Convert to [0,1] by dividing by 100, then StandardScaler (or MinMaxScaler).

OS load normalized

os_load_per_core is already normalized; still scale with StandardScaler.

Time features (add in Python after export)

From ts create:

hour = EXTRACT(HOUR) and encode cyclically:

hour_sin = sin(2π*hour/24), hour_cos = cos(2π*hour/24)

dow = day of week:

dow_sin = sin(2π*dow/7), dow_cos = cos(2π*dow/7)

Lagged features (very helpful for spikes)

Create in Python:

y_lag1 = y.shift(1), y_lag2, y_lag3

Rolling stats: y_roll_mean_3, y_roll_max_3, y_roll_std_6 (choose window = number of snapshots).

⚙️ Fit, CV, Evaluate (sklearn)
import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error

# df = dataframe from the SQL (indexed by 'ts')
y = df['y_cpu_util_pct'] / 100.0  # scale to 0-1 (optional)

# Build feature matrix X (plus engineered time features & lags)
df['hour'] = df.index.hour
df['dow']  = df.index.dayofweek
df['hour_sin'] = np.sin(2*np.pi*df['hour']/24)
df['hour_cos'] = np.cos(2*np.pi*df['hour']/24)
df['dow_sin']  = np.sin(2*np.pi*df['dow']/7)
df['dow_cos']  = np.cos(2*np.pi*df['dow']/7)

# Lags of the target (shifted)
df['y_lag1'] = y.shift(1)
df['y_lag2'] = y.shift(2)
df['y_roll_mean_3'] = y.rolling(3).mean()
df['y_roll_max_3']  = y.rolling(3).max()

# Drop rows with NaNs from lags/rolls
X = df.drop(columns=['y_cpu_util_pct']).dropna()
y_aligned = y.loc[X.index]

pipe = Pipeline([
    ("scaler", StandardScaler()),
    ("mlp", MLPRegressor(hidden_layer_sizes=(128,64),
                         activation='relu',
                         solver='adam',
                         learning_rate='adaptive',
                         max_iter=800,
                         random_state=42))
])

tscv = TimeSeriesSplit(n_splits=5)
maes, rmses = [], []
for train_idx, test_idx in tscv.split(X):
    pipe.fit(X.iloc[train_idx], y_aligned.iloc[train_idx])
    pred = pipe.predict(X.iloc[test_idx])
    mae  = mean_absolute_error(y_aligned.iloc[test_idx], pred)
    rmse = mean_squared_error(y_aligned.iloc[test_idx], pred, squared=False)
    maes.append(mae); rmses.append(rmse)

print("CV MAE:", np.mean(maes))
print("CV RMSE:", np.mean(rmses))

Notes & practical tips

RAC: the query aggregates per end_time. If you want per-instance models, train on the inst_level CTE instead.

Intervals: AWR snapshots are usually 1 hour by default; DBA_HIST_SYSMETRIC_SUMMARY internally holds 1-min/5-min rollups, but we’re aligning metrics by snapshot via joins. If you want finer granularity (e.g., every 5 minutes), switch the “time spine” to DBA_HIST_SYSMETRIC_SUMMARY and join others to its (begin_time,end_time) windows.

Feature drift: occasionally metric names differ slightly across versions; if any NULL columns show up, NVL/COALESCE or adjust the metric names accordingly.

Diagnostics Pack: All DBA_HIST_* requires the license. If you must avoid it, the same logic works on V$* views for short retention.

If you’d like, I can also supply a 5-minute-granularity version that uses DBA_HIST_SYSMETRIC_SUMMARY as the timeline (more predictive for near-term spikes).

