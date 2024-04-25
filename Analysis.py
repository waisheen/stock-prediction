import gspread
import numpy as np
import pandas as pd

from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dropout, Dense

from Query import Query

class Analysis:
    def __init__(self, conn_id: str) -> None:
        self.query = Query(conn_id)

    def exit(self) -> bool:
        self.query.exit()
    
    def process_moving_averages_for_stock(self, stock_symbol: str, window_size: int = 5) -> pd.DataFrame:
        ''' Prepare data for a single stock with moving average feature. '''
        df_price = self.query.avg_stock_price(stock_symbol)
        df_sentiment = self.query.avg_daily_sentiment(stock_symbol)
        df_merged = pd.merge(df_price, df_sentiment, on=['date', 'company_name'], how='inner')

        # filter for a specific company
        df_filtered = df_merged[df_merged['stock_symbol'] == stock_symbol]

        # convert 'date' column to datetime and set as index
        df_filtered['date'] = pd.to_datetime(df_filtered['date'])
        df_filtered.set_index('date', inplace=True)

        # calculate moving average
        df_filtered['moving_average'] = df_filtered['average_stock_price'].rolling(window=window_size, min_periods=1).mean()

        # fill NaN values in 'moving_average' column with forward fill
        df_filtered['moving_average'].fillna(method='ffill', inplace=True)

        # fill NaN values in other columns with forward fill
        df_filtered.fillna(method='ffill', inplace=True)

        return df_filtered[['sentiment_value', 'moving_average']], df_filtered['average_stock_price'].tolist()
    
    def create_dataset(self, X: list, y: list, time_steps: int = 1):
        ''' Create dataset for LSTM. '''
        Xs, ys = [], []
        for i in range(len(X) - time_steps):
            v = X[i:(i + time_steps)]
            Xs.append(v)
            ys.append(y[i + time_steps])
        return np.array(Xs), np.array(ys)
    
    def train_lstm(self, X_train, y_train):
        ''' Train LSTM model.'''
        model = Sequential([
            LSTM(50, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])),
            Dropout(0.2),
            LSTM(50),
            Dropout(0.2),
            Dense(1)
        ])
        model.compile(optimizer='adam', loss='mean_squared_error')
        model.fit(X_train, y_train, epochs=100, batch_size=32, verbose=1)
        return model

    def evaluate_model(self, y_true, y_pred):
        ''' Evaluate model performance by calculating MAE, MSE, RMSE, and R2 score.'''
        mae = mean_absolute_error(y_true, y_pred)
        mse = mean_squared_error(y_true, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_true, y_pred)
        return mae, mse, rmse, r2

    def train_and_predict(self, transformed_data: dict):
        results = []

        for company, value_dict in transformed_data.items():
            features = value_dict['features']
            target = value_dict['target']
            
            print(f"Training model for {company}")

            # Scale the features and target
            scaler_features = MinMaxScaler(feature_range=(0, 1))
            scaler_target = MinMaxScaler(feature_range=(0, 1))
            scaled_features = scaler_features.fit_transform(features)
            scaled_target = scaler_target.fit_transform(np.array(target).reshape(-1, 1))

            # Create dataset for LSTM
            time_steps = 7
            X, y = self.create_dataset(scaled_features, scaled_target, time_steps)
            train_size = int(len(X) * 0.8)
            X_train, X_test = X[:train_size], X[train_size:]
            y_train, y_test = y[:train_size], y[train_size:]
            
            # Train LSTM model
            model = self.train_lstm(X_train, y_train)
            y_pred_scaled = model.predict(X)
            y_pred = scaler_target.inverse_transform(y_pred_scaled)
            dates = features.index[time_steps:]

            # Store predictions
            for date, actual, predicted in zip(dates, scaler_target.inverse_transform(y), y_pred):
                results.append((date, actual[0], predicted[0], company))

            # Forecast future prices
            last_sequence = scaled_features[-time_steps:]
            last_sequence = np.expand_dims(last_sequence, axis=0)
            future_scaled = model.predict(last_sequence)
            future = scaler_target.inverse_transform(future_scaled)
            future_dates = pd.date_range(start=dates[-1], periods=2, freq='B')
            results.append((future_dates[1], None, future[0][0], company))

        all_results = pd.DataFrame(results, columns=['Date', 'Actual', 'Predicted', 'Stock'])
        return all_results
    
    def store_results(self, df_results: pd.DataFrame, filename: str):
        ''' Store the actual and predicted values into Google Sheets. '''
        # Sort DF first
        df_results = df_results.sort_values(by='Date', ascending=False)
        
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

        credentials = ServiceAccountCredentials.from_json_keyfile_name('token.json', scope)
        gc = gspread.authorize(credentials)
        
        try:
            sh = gc.open(filename, folder_id='1KMjzh3JD60RrQgekdNzUnahtxwP-tdpC')
            # access first worksheet
            worksheet = sh.get_worksheet(0)
            #  clear existing content
            worksheet.clear()
        except gspread.SpreadsheetNotFound:
            print('Folder not found')
            # create new spreadsheet
            sh = gc.create(filename, folder_id='1KMjzh3JD60RrQgekdNzUnahtxwP-tdpC')
            worksheet = sh.get_worksheet(0)
            if worksheet is None:
                worksheet = sh.add_worksheet(title="Sheet1", rows="10000", cols="20")

        set_with_dataframe(worksheet, df_results, include_index=False)
        print(f'Successfully stored {len(df_results)}.')
        return