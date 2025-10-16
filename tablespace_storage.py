import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class TablespaceStoragePredictor:
    """
    Predicts tablespace storage usage over time using LSTM neural networks.
    Helps forecast when tablespaces will fill up.
    """
    
    def __init__(self, ora_connection, db_name, lookback_days=90):
        """
        Initialize the predictor.
        
        Args:
            ora_connection: Oracle connection object with query method
            db_name: Database name for queries
            lookback_days: Number of historical days to query
        """
        self.ora = ora_connection
        self.db_name = db_name
        self.lookback_days = lookback_days
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.model = None
        self.history = None
        
    def get_tablespace_history_query(self):
        """
        Oracle query to get tablespace usage over time.
        Uses DBA_HIST views for historical data and current views for real-time data.
        """
        query = f"""SELECT 
            ts.name tablespace_name,
            sn.instance_number as inst_id,
            sn.begin_interval_time as sample_time,
            tsu.tablespace_size/1024/1024/1024 as total_size_gb,
            tsu.tablespace_usedsize/1024/1024/1024 as used_size_gb,
            ROUND((tsu.tablespace_usedsize/tsu.tablespace_size) * 100, 2) as pct_used
        FROM 
            dba_hist_tbspc_space_usage tsu
            JOIN dba_hist_snapshot sn 
                ON tsu.snap_id = sn.snap_id
            JOIN V$tablespace ts
                ON tsu.tablespace_id = ts.ts#
        WHERE 
            sn.begin_interval_time >= SYSDATE - {self.lookback_days}
            AND tsu.tablespace_size > 0
        ORDER BY 
            ts.name, sn.begin_interval_time
        """
        return query
    
    def fetch_data(self):
        """Fetch tablespace usage data from Oracle."""
        query = self.get_tablespace_history_query()
        df = self.ora.query(query, self.db_name)
        
        # Convert sample_time to datetime
        df['sample_time'] = pd.to_datetime(df['SAMPLE_TIME'])
        df = df.rename(columns={
            'TABLESPACE_NAME': 'tablespace_name',
            'INST_ID': 'inst_id',
            'TOTAL_SIZE_GB': 'total_size_gb',
            'USED_SIZE_GB': 'used_size_gb',
            'PCT_USED': 'pct_used'
        })
        
        return df
    
    def preprocess_data(self, df, tablespace_name, lookback=7, forecast_horizon=30):
        """
        Preprocess data for LSTM model.
        
        Args:
            df: DataFrame with tablespace usage
            tablespace_name: Specific tablespace to analyze
            lookback: Number of time steps to look back
            forecast_horizon: Number of days to forecast
        """
        # Filter for specific tablespace
        ts_df = df[df['tablespace_name'] == tablespace_name].copy()
        ts_df = ts_df.sort_values('sample_time')
        
        # Aggregate by day (in case of multiple samples per day)
        ts_df['date'] = ts_df['sample_time'].dt.date
        daily_df = ts_df.groupby('date').agg({
            'used_size_gb': 'max',
            'total_size_gb': 'max',
            'pct_used': 'max'
        }).reset_index()
        
        # Fill missing dates with forward fill
        date_range = pd.date_range(
            start=daily_df['date'].min(),
            end=daily_df['date'].max(),
            freq='D'
        )
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        daily_df = daily_df.set_index('date').reindex(date_range).ffill().reset_index()
        daily_df = daily_df.rename(columns={'index': 'date'})
        
        # Scale the data
        values = daily_df['pct_used'].values.reshape(-1, 1)
        scaled_values = self.scaler.fit_transform(values)
        
        # Create sequences
        X, y = [], []
        for i in range(lookback, len(scaled_values)):
            X.append(scaled_values[i-lookback:i, 0])
            y.append(scaled_values[i, 0])
        
        X, y = np.array(X), np.array(y)
        X = X.reshape((X.shape[0], X.shape[1], 1))
        
        # Split into train/test (80/20)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        
        return X_train, X_test, y_train, y_test, daily_df
    
    def build_model(self, lookback=7):
        """Build LSTM model for time series prediction."""
        model = Sequential([
            LSTM(64, activation='relu', return_sequences=True, 
                 input_shape=(lookback, 1)),
            Dropout(0.2),
            LSTM(32, activation='relu', return_sequences=False),
            Dropout(0.2),
            Dense(16, activation='relu'),
            Dense(1)
        ])
        
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae']
        )
        
        return model
    
    def train(self, X_train, y_train, X_test, y_test, epochs=100, batch_size=32):
        """Train the LSTM model."""
        early_stop = EarlyStopping(
            monitor='val_loss',
            patience=15,
            restore_best_weights=True
        )
        
        self.history = self.model.fit(
            X_train, y_train,
            validation_data=(X_test, y_test),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[early_stop],
            verbose=1
        )
        
        return self.history
    
    def evaluate(self, X_test, y_test):
        """Evaluate model performance."""
        predictions = self.model.predict(X_test)
        
        # Inverse transform to get actual values
        y_test_actual = self.scaler.inverse_transform(y_test.reshape(-1, 1))
        predictions_actual = self.scaler.inverse_transform(predictions)
        
        mae = mean_absolute_error(y_test_actual, predictions_actual)
        rmse = np.sqrt(mean_squared_error(y_test_actual, predictions_actual))
        r2 = r2_score(y_test_actual, predictions_actual)
        
        print(f"\nModel Evaluation Metrics:")
        print(f"MAE: {mae:.2f}%")
        print(f"RMSE: {rmse:.2f}%")
        print(f"R² Score: {r2:.4f}")
        
        return {
            'mae': mae,
            'rmse': rmse,
            'r2': r2,
            'predictions': predictions_actual,
            'actual': y_test_actual
        }
    
    def forecast_future(self, daily_df, lookback=7, forecast_days=30):
        """Forecast future tablespace usage."""
        # Get last lookback days
        last_sequence = daily_df['pct_used'].values[-lookback:]
        last_sequence_scaled = self.scaler.transform(last_sequence.reshape(-1, 1))
        
        predictions = []
        current_sequence = last_sequence_scaled.flatten()
        
        for _ in range(forecast_days):
            # Reshape for prediction
            current_input = current_sequence[-lookback:].reshape(1, lookback, 1)
            
            # Predict next value
            next_pred = self.model.predict(current_input, verbose=0)
            predictions.append(next_pred[0, 0])
            
            # Update sequence
            current_sequence = np.append(current_sequence, next_pred[0, 0])
        
        # Inverse transform predictions
        predictions = np.array(predictions).reshape(-1, 1)
        predictions_actual = self.scaler.inverse_transform(predictions)
        
        # Create forecast dataframe
        last_date = daily_df['date'].max()
        forecast_dates = pd.date_range(
            start=last_date + timedelta(days=1),
            periods=forecast_days,
            freq='D'
        )
        
        forecast_df = pd.DataFrame({
            'date': forecast_dates,
            'predicted_pct_used': predictions_actual.flatten()
        })
        
        return forecast_df
    
    def predict_full_date(self, tablespace_name, forecast_df):
        """
        Predict when tablespace will reach 100% capacity.
        """
        # Find when it crosses 95% (critical threshold)
        critical_rows = forecast_df[forecast_df['predicted_pct_used'] >= 95]
        
        if len(critical_rows) > 0:
            critical_date = critical_rows.iloc[0]['date']
            days_until_critical = (critical_date - pd.Timestamp.now()).days
            print(f"\n⚠️  WARNING: {tablespace_name} will reach 95% capacity on {critical_date.date()}")
            print(f"   Days until critical: {days_until_critical}")
        
        # Find when it reaches 100%
        full_rows = forecast_df[forecast_df['predicted_pct_used'] >= 100]
        if len(full_rows) > 0:
            full_date = full_rows.iloc[0]['date']
            days_until_full = (full_date - pd.Timestamp.now()).days
            print(f"\n🚨 CRITICAL: {tablespace_name} will be full on {full_date.date()}")
            print(f"   Days until full: {days_until_full}")
        else:
            print(f"\n✓ {tablespace_name} will not reach capacity in forecast period")
    
    def run_analysis(self, tablespace_name, lookback=7, forecast_days=30):
        """Run complete analysis pipeline."""
        print(f"Analyzing tablespace: {tablespace_name}")
        print(f"Fetching data for last {self.lookback_days} days...")
        
        # Fetch data
        df = self.fetch_data()
        
        # Preprocess
        print("Preprocessing data...")
        X_train, X_test, y_train, y_test, daily_df = self.preprocess_data(
            df, tablespace_name, lookback, forecast_days
        )
        
        print(f"Training samples: {len(X_train)}, Test samples: {len(X_test)}")
        
        # Build and train model
        print("Building LSTM model...")
        self.model = self.build_model(lookback)
        
        print("Training model...")
        self.train(X_train, y_train, X_test, y_test)
        
        # Evaluate
        print("\nEvaluating model...")
        eval_results = self.evaluate(X_test, y_test)
        
        # Forecast
        print(f"\nForecasting next {forecast_days} days...")
        forecast_df = self.forecast_future(daily_df, lookback, forecast_days)
        
        # Predict when full
        self.predict_full_date(tablespace_name, forecast_df)
        
        return {
            'daily_df': daily_df,
            'forecast_df': forecast_df,
            'eval_results': eval_results,
            'model': self.model
        }
    
    def plot_results(self, daily_df, forecast_df, eval_results, tablespace_name):
        """Plot historical data, predictions, and forecast using Plotly."""
        # Create subplots
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                f'{tablespace_name}: Model Validation (Actual vs Predicted)',
                f'{tablespace_name}: Storage Forecast (Next 30 Days)'
            ),
            vertical_spacing=0.12,
            row_heights=[0.5, 0.5]
        )
        
        # Plot 1: Test predictions vs actual
        test_size = len(eval_results['actual'])
        test_dates = daily_df['date'].values[-test_size:]
        
        fig.add_trace(
            go.Scatter(
                x=test_dates,
                y=eval_results['actual'].flatten(),
                mode='lines',
                name='Actual Usage',
                line=dict(color='blue', width=2),
                showlegend=True
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=test_dates,
                y=eval_results['predictions'].flatten(),
                mode='lines',
                name='Predicted Usage',
                line=dict(color='red', width=2),
                showlegend=True
            ),
            row=1, col=1
        )
        
        # Add shaded area between actual and predicted
        fig.add_trace(
            go.Scatter(
                x=np.concatenate([test_dates, test_dates[::-1]]),
                y=np.concatenate([
                    eval_results['actual'].flatten(),
                    eval_results['predictions'].flatten()[::-1]
                ]),
                fill='toself',
                fillcolor='rgba(128, 128, 128, 0.3)',
                line=dict(color='rgba(255,255,255,0)'),
                showlegend=False,
                name='Prediction Error'
            ),
            row=1, col=1
        )
        
        # Plot 2: Historical + Forecast
        last_30_days = daily_df.tail(30)
        
        fig.add_trace(
            go.Scatter(
                x=last_30_days['date'],
                y=last_30_days['pct_used'],
                mode='lines',
                name='Historical Usage',
                line=dict(color='blue', width=2),
                showlegend=True
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=forecast_df['date'],
                y=forecast_df['predicted_pct_used'],
                mode='lines',
                name='Forecasted Usage',
                line=dict(color='green', width=2),
                showlegend=True
            ),
            row=2, col=1
        )
        
        # Add threshold lines
        all_dates = pd.concat([last_30_days['date'], forecast_df['date']])
        
        fig.add_trace(
            go.Scatter(
                x=[all_dates.min(), all_dates.max()],
                y=[95, 95],
                mode='lines',
                name='Critical (95%)',
                line=dict(color='orange', width=2, dash='dash'),
                showlegend=True
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=[all_dates.min(), all_dates.max()],
                y=[100, 100],
                mode='lines',
                name='Full (100%)',
                line=dict(color='red', width=2, dash='dash'),
                showlegend=True
            ),
            row=2, col=1
        )
        
        # Update layout
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_xaxes(title_text="Date", row=2, col=1)
        fig.update_yaxes(title_text="Usage (%)", row=1, col=1)
        fig.update_yaxes(title_text="Usage (%)", row=2, col=1)
        
        fig.update_layout(
            height=900,
            showlegend=True,
            hovermode='x unified',
            template='plotly_white',
            title_text=f"Tablespace Storage Analysis: {tablespace_name}",
            title_x=0.5
        )
        
        return fig


# Example usage:
"""
# Initialize predictor
predictor = TablespaceStoragePredictor(
    ora_connection=ora,  # Your oracle connection object
    db_name='PROD',
    lookback_days=90
)

# Run analysis for a specific tablespace
results = predictor.run_analysis(
    tablespace_name='USERS',
    lookback=7,
    forecast_days=30
)

# Access results
daily_history = results['daily_df']
forecast = results['forecast_df']

# Plot results with Plotly
fig = predictor.plot_results(
    results['daily_df'],
    results['forecast_df'],
    results['eval_results'],
    'USERS'
)
fig.show()
"""
