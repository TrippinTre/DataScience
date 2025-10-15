"""
Forecast **overall host CPU utilization (%)** from Oracle memory views (no AWR)
- Samples (G)V$OSSTAT and computes CPU utilization from BUSY_TIME / IDLE_TIME deltas
- Aggregates across RAC instances for a cluster-wide % utilization
- Trains a small LSTM to forecast the next N intervals
- Plots interactive Plotly HTML (history + forecast)
- Uses your helper: conn = ora.connect_to_oracle(DB_NAME)
- No CONFIG dict; just variables below

REQUIREMENTS:
    pip install pandas numpy scikit-learn tensorflow plotly
NOTES:
    - (G)V$OSSTAT.BUSY_TIME and IDLE_TIME are cumulative CPU times (centiseconds).
      Utilization over an interval = busy_delta / (busy_delta + idle_delta) * 100.
"""

import os
import sys
import time
import json
import numpy as np
import pandas as pd

import ora  # your module providing: ora.connect_to_oracle(db_name)

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error

import tensorflow as tf
from tensorflow.keras import Sequential
from tensorflow.keras.layers import LSTM, Dense

import plotly.graph_objects as go
from plotly.offline import plot as plotly_save


# =========================
# ====== VARIABLES ========
# =========================
DB_NAME              = "ORCLPDB1"     # passed to ora.connect_to_oracle()
USE_GV               = True           # True -> use GV$OSSTAT (RAC); False -> V$OSSTAT

SAMPLE_INTERVAL_SEC  = 60             # seconds between samples
SAMPLES              = 180            # total samples (e.g., 180 @ 60s ≈ 3 hours)

LOOKBACK             = 30             # timesteps of history -> predict next step
FORECAST_HORIZON     = 30             # steps to forecast (each step = your sampling interval)
EPOCHS               = 25
BATCH_SIZE           = 32
RANDOM_SEED          = 42

THRESHOLD_ALERT_PCT  = 85.0           # optional horizontal line on the chart

OUTPUT_DIR           = "./output_overall_cpu_mem"
SAVE_CSV             = True
SAVE_HTML            = True


# =========================
# ===== SQL STATEMENTS ====
# =========================
# Pull the cumulative BUSY_TIME and IDLE_TIME from (G)V$OSSTAT.
SQL_SAMPLE_GV = """
SELECT
    CAST(SYSTIMESTAMP AT LOCAL AS DATE) AS sample_time,
    s.inst_id,
    s.stat_name,
    s.value
FROM gv$osstat s
WHERE s.stat_name IN ('BUSY_TIME','IDLE_TIME')
"""

SQL_SAMPLE_V = """
SELECT
    CAST(SYSTIMESTAMP AT LOCAL AS DATE) AS sample_time,
    1 AS inst_id,
    s.stat_name,
    s.value
FROM v$osstat s
WHERE s.stat_name IN ('BUSY_TIME','IDLE_TIME')
"""


# =========================
# ===== Utilities =========
# =========================
def set_seeds(seed: int = 42):
    np.random.seed(seed)
    tf.random.set_seed(seed)


def detect_gv_available(conn) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gv$instance")
            cur.fetchone()
        return True
    except Exception:
        return False


def fetch_osstat_sample(conn, use_gv: bool) -> pd.DataFrame:
    sql = SQL_SAMPLE_GV if use_gv else SQL_SAMPLE_V
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def sample_loop(conn, use_gv: bool, interval_sec: int, samples: int) -> pd.DataFrame:
    """
    Repeatedly sample BUSY_TIME and IDLE_TIME (cumulative, centiseconds) per instance.
    Returns concatenated DataFrame of all samples.
    """
    frames = []
    for i in range(samples):
        df = fetch_osstat_sample(conn, use_gv)
        if not df.empty:
            frames.append(df)
        if i < samples - 1:
            time.sleep(interval_sec)
    if not frames:
        return pd.DataFrame(columns=["sample_time","inst_id","stat_name","value"])
    return pd.concat(frames, ignore_index=True)


def compute_overall_utilization(samples_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert cumulative BUSY/IDLE centiseconds into per-interval overall utilization (%).
    Steps:
      - For each instance, pivot BUSY/IDLE, sort by time, compute deltas (no negatives).
      - Sum busy_deltas and idle_deltas across all instances for each timestamp.
      - utilization% = 100 * busy_sum / (busy_sum + idle_sum)
    Returns a DataFrame: [sample_time, cpu_util_pct]
    """
    if samples_df.empty:
        return pd.DataFrame(columns=["sample_time","cpu_util_pct"])

    df = samples_df.copy()
    df["sample_time"] = pd.to_datetime(df["sample_time"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")  # centiseconds

    # Pivot to columns BUSY_TIME / IDLE_TIME per inst_id and timestamp
    piv = (df.pivot_table(index=["inst_id","sample_time"], columns="stat_name", values="value", aggfunc="max")
             .reset_index()
             .sort_values(["inst_id","sample_time"]))
    # Compute per-instance deltas
    for col in ["BUSY_TIME","IDLE_TIME"]:
        piv[f"{col}_delta"] = piv.groupby("inst_id")[col].diff().clip(lower=0)

    # Sum deltas across instances per timestamp
    agg = (piv.groupby("sample_time", as_index=False)[["BUSY_TIME_delta","IDLE_TIME_delta"]].sum())
    # Avoid division by zero
    agg["total"] = agg["BUSY_TIME_delta"] + agg["IDLE_TIME_delta"]
    agg = agg[agg["total"] > 0]
    # Utilization (%). BUSY/IDLE are centiseconds, ratio dimensionless.
    agg["cpu_util_pct"] = 100.0 * (agg["BUSY_TIME_delta"] / agg["total"])
    return agg[["sample_time","cpu_util_pct"]]


def build_series(agg_df: pd.DataFrame,
                 interval_sec: int) -> pd.Series:
    """
    Build a continuous time series aligned to the sampling cadence.
    Any missing timestamps are forward-filled (or interpolated) conservatively.
    """
    if agg_df.empty:
        return pd.Series(dtype="float64")

    agg_df = agg_df.sort_values("sample_time")
    start = agg_df["sample_time"].min()
    end = agg_df["sample_time"].max()
    step = pd.Timedelta(seconds=interval_sec)
    full_index = pd.date_range(start, end, freq=step)

    s = (agg_df.set_index("sample_time")["cpu_util_pct"]
                .reindex(full_index)
                .interpolate(limit_direction="both"))
    s.name = "cpu_util_pct"
    return s


def build_sequences(series: pd.Series, lookback: int):
    scaler = MinMaxScaler()
    y_scaled = scaler.fit_transform(series.values.reshape(-1, 1)).astype("float32")
    X, y = [], []
    for i in range(len(y_scaled) - lookback):
        X.append(y_scaled[i:i + lookback])
        y.append(y_scaled[i + lookback])
    X = np.array(X)  # (N, lookback, 1)
    y = np.array(y)  # (N, 1)
    return X, y, scaler


def time_split(X, y, train_ratio=0.8):
    split = int(len(X) * train_ratio)
    return X[:split], X[split:], y[:split], y[split:]


def build_lstm_model(lookback: int) -> tf.keras.Model:
    model = Sequential([
        LSTM(64, input_shape=(lookback, 1)),
        Dense(32, activation="relu"),
        Dense(1)
    ])
    model.compile(optimizer="adam", loss="mse")
    return model


def evaluate_on_test(model, X_test, y_test, scaler) -> dict:
    y_pred_scaled = model.predict(X_test, verbose=0)
    y_true = scaler.inverse_transform(y_test)
    y_pred = scaler.inverse_transform(y_pred_scaled)

    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    denom = np.maximum(1e-6, np.abs(y_true))
    mape = np.mean(np.abs((y_true - y_pred) / denom)) * 100.0

    return {"MAE_pct": float(mae), "RMSE_pct": float(rmse), "MAPE_percent": float(mape)}


def iterative_forecast(model, series: pd.Series, scaler, lookback: int, horizon: int,
                       interval_sec: int) -> pd.Series:
    last_window = scaler.transform(series.values.reshape(-1, 1)).astype("float32")[-lookback:]
    window = last_window.copy()
    preds = []
    for _ in range(horizon):
        x = window.reshape(1, lookback, 1)
        yhat = model.predict(x, verbose=0)[0, 0]
        preds.append(yhat)
        window = np.vstack([window[1:], [[yhat]]])

    forecast_scaled = np.array(preds).reshape(-1, 1)
    forecast = scaler.inverse_transform(forecast_scaled).ravel()

    step = pd.Timedelta(seconds=interval_sec)
    future_idx = pd.date_range(series.index[-1] + step, periods=horizon, freq=step)
    return pd.Series(forecast, index=future_idx, name="pred_cpu_util_pct")


def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)


def plot_plotly_html(history_series: pd.Series,
                     forecast_series: pd.Series,
                     threshold_pct: float,
                     out_dir: str) -> str:
    """Create interactive Plotly HTML chart with history, forecast, and optional threshold."""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=history_series.index, y=history_series.values,
        mode='lines', name='History CPU Util (%)'
    ))

    fig.add_trace(go.Scatter(
        x=forecast_series.index, y=forecast_series.values,
        mode='lines', name='Forecast CPU Util (%)', line=dict(dash='dash')
    ))

    x_min = min(history_series.index.min(), forecast_series.index.min())
    x_max = max(history_series.index.max(), forecast_series.index.max())

    if threshold_pct is not None:
        fig.add_trace(go.Scatter(
            x=[x_min, x_max], y=[threshold_pct, threshold_pct],
            mode='lines', name=f'Threshold {threshold_pct:.0f}%', line=dict(dash='dot')
        ))

    fig.update_layout(
        title="Overall Host CPU Utilization (%) — (G)V$OSSTAT sampling & LSTM forecast",
        xaxis_title="Time",
        yaxis_title="CPU Utilization (%)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
    )

    ensure_outdir(out_dir)
    out_path = os.path.join(out_dir, "overall_cpu_util_forecast.html")
    plotly_save(fig, filename=out_path, auto_open=False, include_plotlyjs="cdn")
    return out_path


def save_artifacts(ts: pd.Series, fcst: pd.Series, metrics: dict, out_dir: str,
                   save_csv: bool = True, save_html: bool = True):
    ensure_outdir(out_dir)

    with open(os.path.join(out_dir, "overall_metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    if save_csv:
        ts.to_frame("cpu_util_pct").to_csv(os.path.join(out_dir, "overall_history_cpu_util.csv"))
        fcst.to_frame("pred_cpu_util_pct").to_csv(os.path.join(out_dir, "overall_forecast_cpu_util.csv"))

    html_path = None
    if save_html:
        html_path = plot_plotly_html(ts, fcst, THRESHOLD_ALERT_PCT, out_dir)

    return html_path


def main():
    set_seeds(RANDOM_SEED)

    print(f"Connecting with ora.connect_to_oracle('{DB_NAME}') ...")
    conn = None
    try:
        conn = ora.connect_to_oracle(DB_NAME)

        use_gv = USE_GV
        if use_gv and not detect_gv_available(conn):
            print("GV$ views not available; falling back to V$OSSTAT.")
            use_gv = False

        print(f"Sampling {( 'GV$' if use_gv else 'V$')}OSSTAT every {SAMPLE_INTERVAL_SEC}s for {SAMPLES} samples ...")
        raw = sample_loop(conn, use_gv, SAMPLE_INTERVAL_SEC, SAMPLES)
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    if raw.empty:
        print("No samples collected. Ensure privileges and activity.")
        sys.exit(2)

    agg = compute_overall_utilization(raw)
    if agg.empty:
        print("Could not compute utilization deltas (no change observed).")
        sys.exit(3)

    ts = build_series(agg, SAMPLE_INTERVAL_SEC)
    if len(ts) < LOOKBACK + 10:
        print("Not enough samples after alignment; increase SAMPLES or reduce LOOKBACK.")
        sys.exit(4)

    # Supervised data
    X, y, scaler = build_sequences(ts, LOOKBACK)
    X_train, X_test, y_train, y_test = time_split(X, y, train_ratio=0.8)

    # Model & train
    model = build_lstm_model(LOOKBACK)
    print("Training LSTM...")
    model.fit(
        X_train, y_train,
        epochs=EPOCHS, batch_size=BATCH_SIZE,
        validation_data=(X_test, y_test),
        verbose=0
    )

    # Evaluation
    metrics = evaluate_on_test(model, X_test, y_test, scaler)
    print(f"Test MAE:  {metrics['MAE_pct']:.2f} pct")
    print(f"Test RMSE: {metrics['RMSE_pct']:.2f} pct")
    print(f"Test MAPE: {metrics['MAPE_percent']:.2f}%")

    # Forecast
    fcst = iterative_forecast(model, ts, scaler, LOOKBACK, FORECAST_HORIZON, SAMPLE_INTERVAL_SEC)

    # Save artifacts
    ensure_outdir(OUTPUT_DIR)
    html_path = save_artifacts(ts, fcst, metrics, OUTPUT_DIR, SAVE_CSV, SAVE_HTML)

    print(f"\nArtifacts directory: {os.path.abspath(OUTPUT_DIR)}")
    if html_path:
        print(f"Interactive chart: {os.path.abspath(html_path)}")
    print("Done.")


if __name__ == "__main__":
    main()