import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class CPUUsagePredictor:
    """
    Predicts CPU usage percentage for executed queries using LSTM neural networks.
    Analyzes 2 weeks to 1 month of historical data and forecasts next 7 days.
    """
    
    def __init__(self, ora_connection, db_name, lookback_days=30):
        """
        Initialize the CPU predictor.
        
        Args:
            ora_connection: Oracle connection object with query method
            db_name: Database name for queries
            lookback_days: Number of historical days (14-30 recommended)
        """
        self.ora = ora_connection
        self.db_name = db_name
        self.lookback_days = lookback_days
        self.scaler_cpu = MinMaxScaler(feature_range=(0, 1))
        self.scaler_features = StandardScaler()
        self.model = None
        self.history = None
        
    def get_cpu_usage_query(self):
        """
        Oracle query to get CPU usage for executed queries over time.
        Uses GV$ views for real-time data and AWR views for historical trends.
        """
        query = f"""
        WITH current_cpu AS (
            SELECT 
                s.inst_id,
                TRUNC(SYSDATE, 'HH24') as sample_hour,
                AVG(s.cpu_time / GREATEST(s.elapsed_time, 1) * 100) as avg_cpu_pct,
                MAX(s.cpu_time / GREATEST(s.elapsed_time, 1) * 100) as max_cpu_pct,
                COUNT(*) as query_count,
                AVG(s.elapsed_time/1000000) as avg_elapsed_sec,
                SUM(s.disk_reads) as total_disk_reads,
                SUM(s.buffer_gets) as total_buffer_gets,
                AVG(s.executions) as avg_executions
            FROM 
                gv$sql s
            WHERE 
                s.last_active_time >= SYSDATE - INTERVAL '1' HOUR
                AND s.elapsed_time > 0
                AND s.command_type IN (2, 3, 6, 7, 189)  -- SELECT, INSERT, UPDATE, DELETE, MERGE
            GROUP BY 
                s.inst_id, TRUNC(SYSDATE, 'HH24')
        ),
        historical_cpu AS (
            SELECT 
                h.instance_number as inst_id,
                TRUNC(s.begin_interval_time, 'HH24') as sample_hour,
                AVG(
                    (h.cpu_time_delta / GREATEST(h.elapsed_time_delta, 1)) * 100
                ) as avg_cpu_pct,
                MAX(
                    (h.cpu_time_delta / GREATEST(h.elapsed_time_delta, 1)) * 100
                ) as max_cpu_pct,
                COUNT(*) as query_count,
                AVG(h.elapsed_time_delta/1000000/GREATEST(h.executions_delta, 1)) as avg_elapsed_sec,
                SUM(h.disk_reads_delta) as total_disk_reads,
                SUM(h.buffer_gets_delta) as total_buffer_gets,
                AVG(h.executions_delta) as avg_executions
            FROM 
                dba_hist_sqlstat h
                JOIN dba_hist_snapshot s ON h.snap_id = s.snap_id 
                    AND h.instance_number = s.instance_number
            WHERE 
                s.begin_interval_time >= SYSDATE - {self.lookback_days}
                AND h.elapsed_time_delta > 0
                AND h.executions_delta > 0
            GROUP BY 
                h.instance_number, TRUNC(s.begin_interval_time, 'HH24')
        ),
        system_cpu AS (
            SELECT
                h.instance_number as inst_id,
                TRUNC(s.begin_interval_time, 'HH24') as sample_hour,
                AVG(h.value) as sys_cpu_pct
            FROM
                dba_hist_sysmetric_summary h
                JOIN dba_hist_snapshot s ON h.snap_id = s.snap_id 
                    AND h.instance_number = s.instance_number
            WHERE
                h.metric_name = 'Host CPU Utilization (%)'
                AND s.begin_interval_time >= SYSDATE - {self.lookback_days}
            GROUP BY
                h.instance_number, TRUNC(s.begin_interval_time, 'HH24')
        )
        SELECT 
            h.inst_id,
            h.sample_hour,
            h.avg_cpu_pct,
            h.max_cpu_pct,
            h.query_count,
            h.avg_elapsed_sec,
            h.total_disk_reads,
            h.total_buffer_gets,
            h.avg_executions,
            NVL(sc.sys_cpu_pct, 0) as system_cpu_pct
        FROM historical_cpu h
        LEFT JOIN system_cpu sc ON h.inst_id = sc.inst_id 
            AND h.sample_hour = sc.sample_hour
        UNION ALL
        SELECT 
            c.inst_id,
            c.sample_hour,
            c.avg_cpu_pct,
            c.max_cpu_pct,
            c.query_count,
            c.avg_elapsed_sec,
            c.total_disk_reads,
            c.total_buffer_gets,
            c.avg_executions,
            (SELECT value FROM gv$sysmetric 
             WHERE metric_name = 'Host CPU Utilization (%)'
             AND inst_id = c.inst_id
             AND ROWNUM = 1) as system_cpu_pct
        FROM current_cpu c
        ORDER BY sample_hour, inst_id
        """
        return query
    
    def fetch_data(self):
        """Fetch CPU usage data from Oracle."""
        query = self.get_cpu_usage_query()
        df = self.ora.query(query, self.db_name)
        
        # Convert and rename columns
        df['sample_hour'] = pd.to_datetime(df['SAMPLE_HOUR'])
        df = df.rename(columns={
            'INST_ID': 'inst_id',
            'AVG_CPU_PCT': 'avg_cpu_pct',
            'MAX_CPU_PCT': 'max_cpu_pct',
            'QUERY_COUNT': 'query_count',
            'AVG_ELAPSED_SEC': 'avg_elapsed_sec',
            'TOTAL_DISK_READS': 'total_disk_reads',
            'TOTAL_BUFFER_GETS': 'total_buffer_gets',
            'AVG_EXECUTIONS': 'avg_executions',
            'SYSTEM_CPU_PCT': 'system_cpu_pct'
        })
        
        # Handle nulls and clean data
        df = df.fillna(0)
        df['avg_cpu_pct'] = df['avg_cpu_pct'].clip(0, 100)
        df['max_cpu_pct'] = df['max_cpu_pct'].clip(0, 100)
        
        return df
    
    def engineer_features(self, df):
        """Create additional time-based and statistical features."""
        df = df.copy()
        
        # Time-based features
        df['hour'] = df['sample_hour'].dt.hour
        df['day_of_week'] = df['sample_hour'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_business_hours'] = ((df['hour'] >= 8) & (df['hour'] <= 18)).astype(int)
        
        # Lag features
        df = df.sort_values('sample_hour')
        for lag in [1, 3, 6, 12, 24]:
            df[f'cpu_lag_{lag}h'] = df.groupby('inst_id')['avg_cpu_pct'].shift(lag)
        
        # Rolling statistics
        for window in [6, 12, 24]:
            df[f'cpu_rolling_mean_{window}h'] = df.groupby('inst_id')['avg_cpu_pct'].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean()
            )
            df[f'cpu_rolling_std_{window}h'] = df.groupby('inst_id')['avg_cpu_pct'].transform(
                lambda x: x.rolling(window=window, min_periods=1).std()
            ).fillna(0)
        
        # Query intensity features
        df['queries_per_minute'] = df['query_count'] / 60
        df['disk_reads_per_query'] = df['total_disk_reads'] / df['query_count'].replace(0, 1)
        df['buffer_gets_per_query'] = df['total_buffer_gets'] / df['query_count'].replace(0, 1)
        
        # Fill any remaining NaN values
        df = df.fillna(method='ffill').fillna(0)
        
        return df
    
    def preprocess_data(self, df, lookback=24, forecast_horizon=168):
        """
        Preprocess data for LSTM model.
        
        Args:
            df: DataFrame with CPU usage
            lookback: Number of hours to look back (24 = 1 day)
            forecast_horizon: Hours to forecast (168 = 7 days)
        """
        # Engineer features
        df = self.engineer_features(df)
        
        # Select features for prediction
        feature_cols = [
            'avg_cpu_pct', 'max_cpu_pct', 'system_cpu_pct',
            'query_count', 'avg_elapsed_sec',
            'hour', 'day_of_week', 'is_business_hours',
            'cpu_lag_1h', 'cpu_lag_3h', 'cpu_lag_6h',
            'cpu_rolling_mean_6h', 'cpu_rolling_std_6h',
            'queries_per_minute', 'disk_reads_per_query'
        ]
        
        # Aggregate by hour (across instances if multiple)
        hourly_df = df.groupby('sample_hour').agg({
            'avg_cpu_pct': 'mean',
            'max_cpu_pct': 'max',
            'system_cpu_pct': 'mean',
            'query_count': 'sum',
            'avg_elapsed_sec': 'mean',
            'hour': 'first',
            'day_of_week': 'first',
            'is_business_hours': 'first',
            'cpu_lag_1h': 'mean',
            'cpu_lag_3h': 'mean',
            'cpu_lag_6h': 'mean',
            'cpu_rolling_mean_6h': 'mean',
            'cpu_rolling_std_6h': 'mean',
            'queries_per_minute': 'sum',
            'disk_reads_per_query': 'mean'
        }).reset_index()
        
        hourly_df = hourly_df.sort_values('sample_hour')
        
        # Scale target variable (CPU)
        cpu_values = hourly_df['avg_cpu_pct'].values.reshape(-1, 1)
        cpu_scaled = self.scaler_cpu.fit_transform(cpu_values)
        
        # Scale feature variables
        feature_values = hourly_df[feature_cols].values
        features_scaled = self.scaler_features.fit_transform(feature_values)
        
        # Create sequences
        X, y = [], []
        for i in range(lookback, len(features_scaled)):
            X.append(features_scaled[i-lookback:i])
            y.append(cpu_scaled[i, 0])
        
        X, y = np.array(X), np.array(y)
        
        # Split into train/test (80/20)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        
        return X_train, X_test, y_train, y_test, hourly_df, feature_cols
    
    def build_model(self, lookback=24, n_features=15):
        """Build Bidirectional LSTM model for CPU prediction."""
        model = Sequential([
            Bidirectional(LSTM(128, activation='relu', return_sequences=True),
                         input_shape=(lookback, n_features)),
            Dropout(0.3),
            Bidirectional(LSTM(64, activation='relu', return_sequences=True)),
            Dropout(0.3),
            LSTM(32, activation='relu', return_sequences=False),
            Dropout(0.2),
            Dense(32, activation='relu'),
            Dense(16, activation='relu'),
            Dense(1)
        ])
        
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=0.001),
            loss='huber',  # Robust to outliers
            metrics=['mae', 'mse']
        )
        
        return model
    
    def train(self, X_train, y_train, X_test, y_test, epochs=150, batch_size=32):
        """Train the LSTM model with callbacks."""
        early_stop = EarlyStopping(
            monitor='val_loss',
            patience=20,
            restore_best_weights=True,
            verbose=1
        )
        
        reduce_lr = ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.5,
            patience=10,
            min_lr=0.00001,
            verbose=1
        )
        
        self.history = self.model.fit(
            X_train, y_train,
            validation_data=(X_test, y_test),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[early_stop, reduce_lr],
            verbose=1
        )
        
        return self.history
    
    def evaluate(self, X_test, y_test):
        """Evaluate model performance with detailed metrics."""
        predictions = self.model.predict(X_test, verbose=0)
        
        # Inverse transform to get actual CPU percentages
        y_test_actual = self.scaler_cpu.inverse_transform(y_test.reshape(-1, 1))
        predictions_actual = self.scaler_cpu.inverse_transform(predictions)
        
        mae = mean_absolute_error(y_test_actual, predictions_actual)
        rmse = np.sqrt(mean_squared_error(y_test_actual, predictions_actual))
        r2 = r2_score(y_test_actual, predictions_actual)
        mape = np.mean(np.abs((y_test_actual - predictions_actual) / 
                              (y_test_actual + 1e-10))) * 100
        
        print(f"\n{'='*50}")
        print(f"Model Evaluation Metrics:")
        print(f"{'='*50}")
        print(f"MAE (Mean Absolute Error):     {mae:.2f}%")
        print(f"RMSE (Root Mean Squared Error): {rmse:.2f}%")
        print(f"MAPE (Mean Abs Percentage Err): {mape:.2f}%")
        print(f"R² Score:                       {r2:.4f}")
        print(f"{'='*50}\n")
        
        return {
            'mae': mae,
            'rmse': rmse,
            'mape': mape,
            'r2': r2,
            'predictions': predictions_actual.flatten(),
            'actual': y_test_actual.flatten()
        }
    
    def forecast_future(self, hourly_df, feature_cols, lookback=24, forecast_hours=168):
        """
        Forecast future CPU usage for next 7 days (168 hours).
        """
        # Get last lookback hours of data
        last_features = hourly_df[feature_cols].values[-lookback:]
        last_features_scaled = self.scaler_features.transform(last_features)
        
        predictions = []
        current_sequence = last_features_scaled.copy()
        
        last_timestamp = hourly_df['sample_hour'].max()
        
        for i in range(forecast_hours):
            # Prepare input
            current_input = current_sequence[-lookback:].reshape(1, lookback, len(feature_cols))
            
            # Predict next hour
            next_pred = self.model.predict(current_input, verbose=0)
            predictions.append(next_pred[0, 0])
            
            # Create next feature vector
            next_hour = last_timestamp + timedelta(hours=i+1)
            next_features = self._create_future_features(
                next_hour, 
                next_pred[0, 0],
                current_sequence,
                hourly_df
            )
            
            # Scale and append
            next_features_scaled = self.scaler_features.transform(
                next_features.reshape(1, -1)
            )
            current_sequence = np.vstack([current_sequence, next_features_scaled[0]])
        
        # Inverse transform predictions
        predictions = np.array(predictions).reshape(-1, 1)
        predictions_actual = self.scaler_cpu.inverse_transform(predictions)
        
        # Create forecast dataframe
        forecast_dates = pd.date_range(
            start=last_timestamp + timedelta(hours=1),
            periods=forecast_hours,
            freq='H'
        )
        
        forecast_df = pd.DataFrame({
            'sample_hour': forecast_dates,
            'predicted_cpu_pct': predictions_actual.flatten()
        })
        
        return forecast_df
    
    def _create_future_features(self, timestamp, predicted_cpu, sequence, hist_df):
        """Create feature vector for future timestamp."""
        hour = timestamp.hour
        day_of_week = timestamp.dayofweek
        is_business_hours = 1 if 8 <= hour <= 18 else 0
        
        # Use historical patterns for query metrics
        similar_hours = hist_df[
            (hist_df['sample_hour'].dt.hour == hour) & 
            (hist_df['sample_hour'].dt.dayofweek == day_of_week)
        ]
        
        if len(similar_hours) > 0:
            avg_queries = similar_hours['query_count'].mean()
            avg_elapsed = similar_hours['avg_elapsed_sec'].mean()
            avg_disk_reads = similar_hours['disk_reads_per_query'].mean()
        else:
            avg_queries = hist_df['query_count'].mean()
            avg_elapsed = hist_df['avg_elapsed_sec'].mean()
            avg_disk_reads = hist_df['disk_reads_per_query'].mean()
        
        # Create feature array (must match training feature order)
        features = np.array([
            predicted_cpu,  # avg_cpu_pct
            predicted_cpu * 1.2,  # max_cpu_pct (estimate)
            predicted_cpu * 0.9,  # system_cpu_pct (estimate)
            avg_queries,  # query_count
            avg_elapsed,  # avg_elapsed_sec
            hour,  # hour
            day_of_week,  # day_of_week
            is_business_hours,  # is_business_hours
            predicted_cpu,  # cpu_lag_1h (using current prediction)
            predicted_cpu,  # cpu_lag_3h
            predicted_cpu,  # cpu_lag_6h
            predicted_cpu,  # cpu_rolling_mean_6h
            5.0,  # cpu_rolling_std_6h (default variance)
            avg_queries / 60,  # queries_per_minute
            avg_disk_reads  # disk_reads_per_query
        ])
        
        return features
    
    def detect_anomalies(self, forecast_df, threshold_high=80, threshold_critical=95):
        """Detect periods of high CPU usage in forecast."""
        high_cpu = forecast_df[forecast_df['predicted_cpu_pct'] >= threshold_high]
        critical_cpu = forecast_df[forecast_df['predicted_cpu_pct'] >= threshold_critical]
        
        print(f"\n{'='*60}")
        print(f"CPU Usage Forecast Analysis (Next 7 Days)")
        print(f"{'='*60}")
        
        avg_cpu = forecast_df['predicted_cpu_pct'].mean()
        max_cpu = forecast_df['predicted_cpu_pct'].max()
        min_cpu = forecast_df['predicted_cpu_pct'].min()
        
        print(f"\nOverall Statistics:")
        print(f"  Average CPU: {avg_cpu:.2f}%")
        print(f"  Maximum CPU: {max_cpu:.2f}%")
        print(f"  Minimum CPU: {min_cpu:.2f}%")
        
        if len(critical_cpu) > 0:
            print(f"\n🚨 CRITICAL ALERTS:")
            print(f"  {len(critical_cpu)} hours predicted above {threshold_critical}%")
            first_critical = critical_cpu.iloc[0]['sample_hour']
            print(f"  First occurrence: {first_critical}")
            
            # Group consecutive critical periods
            critical_cpu['hour_diff'] = critical_cpu['sample_hour'].diff().dt.total_seconds() / 3600
            critical_cpu['period'] = (critical_cpu['hour_diff'] > 1).cumsum()
            
            print(f"\n  Critical Periods:")
            for period_id, period in critical_cpu.groupby('period'):
                start = period['sample_hour'].min()
                end = period['sample_hour'].max()
                duration = len(period)
                avg = period['predicted_cpu_pct'].mean()
                print(f"    • {start} to {end} ({duration}h, avg {avg:.1f}%)")
        
        if len(high_cpu) > 0:
            print(f"\n⚠️  HIGH CPU WARNINGS:")
            print(f"  {len(high_cpu)} hours predicted above {threshold_high}%")
        
        if len(critical_cpu) == 0 and len(high_cpu) == 0:
            print(f"\n✓ No high CPU periods detected in forecast")
        
        print(f"\n{'='*60}\n")
        
        return {
            'high_cpu_hours': len(high_cpu),
            'critical_cpu_hours': len(critical_cpu),
            'avg_cpu': avg_cpu,
            'max_cpu': max_cpu
        }
    
    def run_analysis(self, lookback=24, forecast_hours=168):
        """
        Run complete CPU prediction analysis.
        
        Args:
            lookback: Hours to look back for prediction (24 = 1 day)
            forecast_hours: Hours to forecast (168 = 7 days)
        """
        print(f"{'='*60}")
        print(f"CPU Usage Prediction Analysis")
        print(f"{'='*60}")
        print(f"Historical period: {self.lookback_days} days")
        print(f"Forecast period: {forecast_hours} hours (7 days)")
        print(f"Lookback window: {lookback} hours\n")
        
        # Fetch data
        print("Fetching CPU usage data from Oracle...")
        df = self.fetch_data()
        print(f"Retrieved {len(df)} records")
        
        # Preprocess
        print("\nPreprocessing and feature engineering...")
        X_train, X_test, y_train, y_test, hourly_df, feature_cols = self.preprocess_data(
            df, lookback, forecast_hours
        )
        
        print(f"Training samples: {len(X_train)}")
        print(f"Test samples: {len(X_test)}")
        print(f"Features: {len(feature_cols)}")
        
        # Build model
        print("\nBuilding Bidirectional LSTM model...")
        self.model = self.build_model(lookback, len(feature_cols))
        print(f"Total parameters: {self.model.count_params():,}")
        
        # Train
        print("\nTraining model...")
        self.train(X_train, y_train, X_test, y_test)
        
        # Evaluate
        print("\nEvaluating model performance...")
        eval_results = self.evaluate(X_test, y_test)
        
        # Forecast
        print(f"Forecasting next {forecast_hours} hours...")
        forecast_df = self.forecast_future(hourly_df, feature_cols, lookback, forecast_hours)
        
        # Analyze forecast
        anomaly_report = self.detect_anomalies(forecast_df)
        
        return {
            'hourly_df': hourly_df,
            'forecast_df': forecast_df,
            'eval_results': eval_results,
            'anomaly_report': anomaly_report,
            'model': self.model,
            'training_history': self.history
        }
    
    def plot_results(self, hourly_df, forecast_df, eval_results):
        """Plot historical data, predictions, and forecast using Plotly."""
        # Create subplots
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                'Model Validation: Actual vs Predicted CPU Usage',
                'CPU Usage Forecast: Next 7 Days'
            ),
            vertical_spacing=0.12,
            row_heights=[0.5, 0.5]
        )
        
        # Plot 1: Test predictions vs actual
        test_size = len(eval_results['actual'])
        test_hours = hourly_df['sample_hour'].values[-test_size:]
        
        fig.add_trace(
            go.Scatter(
                x=test_hours,
                y=eval_results['actual'],
                mode='lines',
                name='Actual CPU %',
                line=dict(color='blue', width=2),
                showlegend=True
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=test_hours,
                y=eval_results['predictions'],
                mode='lines',
                name='Predicted CPU %',
                line=dict(color='red', width=2),
                showlegend=True
            ),
            row=1, col=1
        )
        
        # Add shaded area for prediction error
        fig.add_trace(
            go.Scatter(
                x=np.concatenate([test_hours, test_hours[::-1]]),
                y=np.concatenate([
                    eval_results['actual'],
                    eval_results['predictions'][::-1]
                ]),
                fill='toself',
                fillcolor='rgba(128, 128, 128, 0.3)',
                line=dict(color='rgba(255,255,255,0)'),
                showlegend=False,
                name='Prediction Error'
            ),
            row=1, col=1
        )
        
        # Plot 2: Recent history + Future forecast
        last_24h = hourly_df.tail(24)
        
        fig.add_trace(
            go.Scatter(
                x=last_24h['sample_hour'],
                y=last_24h['avg_cpu_pct'],
                mode='lines',
                name='Recent Historical',
                line=dict(color='blue', width=2),
                showlegend=True
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=forecast_df['sample_hour'],
                y=forecast_df['predicted_cpu_pct'],
                mode='lines',
                name='7-Day Forecast',
                line=dict(color='green', width=2),
                showlegend=True
            ),
            row=2, col=1
        )
        
        # Add threshold lines
        all_times = pd.concat([
            last_24h['sample_hour'],
            forecast_df['sample_hour']
        ])
        
        fig.add_trace(
            go.Scatter(
                x=[all_times.min(), all_times.max()],
                y=[80, 80],
                mode='lines',
                name='High Threshold (80%)',
                line=dict(color='orange', width=2, dash='dash'),
                showlegend=True
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=[all_times.min(), all_times.max()],
                y=[95, 95],
                mode='lines',
                name='Critical Threshold (95%)',
                line=dict(color='red', width=2, dash='dash'),
                showlegend=True
            ),
            row=2, col=1
        )
        
        # Update layout
        fig.update_xaxes(title_text="Time", row=1, col=1)
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_yaxes(title_text="CPU Usage (%)", row=1, col=1)
        fig.update_yaxes(title_text="CPU Usage (%)", row=2, col=1)
        
        fig.update_layout(
            height=900,
            showlegend=True,
            hovermode='x unified',
            template='plotly_white',
            title_text="CPU Usage Prediction Analysis",
            title_x=0.5
        )
        
        return fig


# Example usage:
"""
# Initialize predictor with 2-4 weeks of historical data
predictor = CPUUsagePredictor(
    ora_connection=ora,  # Your oracle connection object
    db_name='PROD',
    lookback_days=30  # Use 14-30 days of history
)

# Run complete analysis
results = predictor.run_analysis(
    lookback=24,  # Look back 24 hours for each prediction
    forecast_hours=168  # Forecast 7 days (168 hours)
)

# Access results
historical_data = results['hourly_df']
forecast = results['forecast_df']
evaluation = results['eval_results']
anomalies = results['anomaly_report']

# Plot results with Plotly
fig = predictor.plot_results(
    results['hourly_df'],
    results['forecast_df'],
    results['eval_results']
)
fig.show()

# Get specific predictions
print(forecast[['sample_hour', 'predicted_cpu_pct']].head(24))
"""
