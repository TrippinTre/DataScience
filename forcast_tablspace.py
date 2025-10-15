"""
Forecast Oracle tablespace usage with an LSTM (end-to-end)
- Uses your helper: conn = ora.connect_to_oracle(DB_NAME)
- Plots with Plotly (interactive HTML)
- No CONFIG dict; simple variables at the top

REQUIREMENTS:
    pip install pandas numpy scikit-learn tensorflow plotly
"""

import os
import sys
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
DB_NAME            = "ORCLPDB1"     # passed to ora.connect_to_oracle()
TABLESPACE_NAME    = "USERS"
DAYS_BACK          = 180            # days of history to pull from AWR

LOOKBACK           = 30             # days of history -> predict next day
FORECAST_HORIZON   = 30             # days to forecast
EPOCHS             = 40
BATCH_SIZE         = 32
RANDOM_SEED        = 42

THRESHOLD_PCT      = 90.0           # capacity alert threshold

OUTPUT_DIR         = "./output"     # artifacts directory
SAVE_CSV           = True
SAVE_HTML          = True


# =========================
# ====== SQL (AWR) ========
# =========================
AWR_SQL = """
/* Build time series of tablespace used% from AWR */
WITH bsize AS (
  SELECT TO_NUMBER(value) AS block_size
  FROM   v$parameter
  WHERE  name = 'db_block_size'
)
SELECT
    CAST(s.begin_interval_time AS DATE)         AS snap_time,
    vt.name                                      AS tablespace_name,
    (u.used_space * b.block_size) / 1048576      AS used_mb,
    (u.tablespace_size * b.block_size) / 1048576 AS size_mb,
    CASE WHEN u.tablespace_size > 0
         THEN (u.used_space / u.tablespace_size) * 100
         ELSE NULL
    END                                          AS used_pct
FROM   dba_hist_tbspc_space_usage u
JOIN   dba_hist_snapshot s
       ON s.snap_id = u.snap_id
      AND s.dbid = u.dbid
      AND s.instance_number = u.instance_number
JOIN   v$tablespace vt
       ON vt.ts# = u.tablespace_id
CROSS JOIN bsize b
WHERE  vt.name = :ts_name
  AND  s.begin_interval_time >= (SYSDATE - :days_back)
ORDER BY s.begin_interval_time
"""


# =========================
# ===== Utilities =========
# =========================
def set_seeds(seed: int = 42):
    np.random.seed(seed)
    tf.random.set_seed(seed)


def fetch_awr_tablespace_usage(conn, ts_name: str, days_back: int) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(AWR_SQL, ts_name=ts_name, days_back=days_back)
        cols = [c[0].lower() for c in cur.description]
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    df["snap_time"] = pd.to_datetime(df["snap_time"])
    return df


def to_daily_series(df: pd.DataFrame) -> pd.Series:
    df = df.sort_values("snap_time").set_index("snap_time")
    ts = df["used_pct"].astype(float).resample("D").mean()
    ts = ts.interpolate(limit_direction="both")
    return ts


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
    denom = np.maximum(1e-3, np.abs(y_true))
    mape = np.mean(np.abs((y_true - y_pred) / denom)) * 100.0

    return {"MAE_pct": float(mae), "RMSE_pct": float(rmse), "MAPE_pct": float(mape)}


def iterative_forecast(model, series: pd.Series, scaler, lookback: int, horizon: int) -> pd.Series:
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

    future_idx = pd.date_range(series.index[-1] + pd.Timedelta(days=1), periods=horizon, freq="D")
    return pd.Series(forecast, index=future_idx, name="pred_used_pct")


def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)


def plot_plotly_html(history_series: pd.Series,
                     forecast_series: pd.Series,
                     tablespace: str,
                     threshold_pct: float,
                     out_dir: str) -> str:
    """Create an interactive Plotly HTML with history, forecast, and threshold line."""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=history_series.index, y=history_series.values,
        mode='lines', name='History (used%)'
    ))

    fig.add_trace(go.Scatter(
        x=forecast_series.index, y=forecast_series.values,
        mode='lines', name='Forecast (used%)', line=dict(dash='dash')
    ))

    # Threshold line across combined date span
    x_min = min(history_series.index.min(), forecast_series.index.min())
    x_max = max(history_series.index.max(), forecast_series.index.max())
    fig.add_trace(go.Scatter(
        x=[x_min, x_max], y=[threshold_pct, threshold_pct],
        mode='lines', name=f'Threshold {threshold_pct:.0f}%', line=dict(dash='dot')
    ))

    fig.update_layout(
        title=f"Tablespace {tablespace} used% — history & LSTM forecast",
        xaxis_title="Date",
        yaxis_title="Used %",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
    )

    ensure_outdir(out_dir)
    out_path = os.path.join(out_dir, "used_pct_forecast.html")
    plotly_save(fig, filename=out_path, auto_open=False, include_plotlyjs="cdn")
    return out_path


def save_artifacts(ts: pd.Series, fcst: pd.Series, metrics: dict, out_dir: str,
                   save_csv: bool = True, save_html: bool = True):
    ensure_outdir(out_dir)

    with open(os.path.join(out_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    if save_csv:
        ts.to_frame("used_pct").to_csv(os.path.join(out_dir, "history_used_pct.csv"))
        fcst.to_frame("pred_used_pct").to_csv(os.path.join(out_dir, "forecast_used_pct.csv"))

    html_path = None
    if save_html:
        html_path = plot_plotly_html(ts, fcst, TABLESPACE_NAME, THRESHOLD_PCT, out_dir)

    return html_path


def find_breach(fcst: pd.Series, threshold: float):
    breach = fcst[fcst >= threshold]
    return None if breach.empty else breach.index[0].date()


def main():
    set_seeds(RANDOM_SEED)

    # ----- CONNECT USING YOUR HELPER -----
    print(f"Connecting with ora.connect_to_oracle('{DB_NAME}') ...")
    conn = None
    try:
        conn = ora.connect_to_oracle(DB_NAME)
        print(f"Querying AWR for tablespace '{TABLESPACE_NAME}' over last {DAYS_BACK} days...")
        df = fetch_awr_tablespace_usage(conn, TABLESPACE_NAME, DAYS_BACK)
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    if df.empty:
        print("No rows returned. Verify permissions, AWR retention, and TABLESPACE_NAME.")
        sys.exit(2)

    # Build daily series
    ts = to_daily_series(df)  # pandas Series of used%
    if ts.isna().all() or len(ts) < LOOKBACK + 10:
        print("Insufficient data after resampling/interpolation. Try increasing DAYS_BACK.")
        sys.exit(3)

    # Sequences
    X, y, scaler = build_sequences(ts, LOOKBACK)
    X_train, X_test, y_train, y_test = time_split(X, y, train_ratio=0.8)

    # Model
    model = build_lstm_model(LOOKBACK)
    print("Training LSTM...")
    model.fit(
        X_train, y_train,
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,
        validation_data=(X_test, y_test),
        verbose=0
    )

    # Evaluation
    metrics = evaluate_on_test(model, X_test, y_test, scaler)
    print(f"Test MAE:  {metrics['MAE_pct']:.2f} pct")
    print(f"Test RMSE: {metrics['RMSE_pct']:.2f} pct")
    print(f"Test MAPE: {metrics['MAPE_pct']:.2f}%")

    # Forecast
    fcst = iterative_forecast(model, ts, scaler, LOOKBACK, FORECAST_HORIZON)
    breach_date = find_breach(fcst, THRESHOLD_PCT)
    if breach_date:
        print(f"⚠️  Projected to cross {THRESHOLD_PCT:.0f}% on {breach_date}")
    else:
        print(f"✅ No {THRESHOLD_PCT:.0f}% breach within {FORECAST_HORIZON} days.")

    # Artifacts
    html_path = save_artifacts(ts, fcst, metrics, OUTPUT_DIR, SAVE_CSV, SAVE_HTML)
    print(f"Artifacts saved in: {os.path.abspath(OUTPUT_DIR)}")
    if html_path:
        print(f"Interactive chart: {os.path.abspath(html_path)}")
    print("Done.")


if __name__ == "__main__":
    main()