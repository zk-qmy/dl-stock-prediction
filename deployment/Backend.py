import sys
import os
import pandas as pd
import numpy as np
import joblib
import tensorflow as tf
from sqlalchemy import create_engine, exc, text, inspect
from src.Logger import logging
from src.Exception import CustomException
from src.etl.SQLManager import SQLConnection


class Backend:
    def __init__(self):
        print("init SQLManager")
        self.connection = SQLConnection().get_db_engine()
        # self.df = self.load_close_data()
        print("Done getting connection from SQL Manager!")

    def fetch_close_data(self, ticker_name):
        query = text("""
            SELECT close
             FROM main_historicalstock
             WHERE ticker = :ticker_name
        """)
        with self.connection.connect() as conn:
            df = pd.read_sql_query(query, conn,
                                   params={"ticker_name": ticker_name})
        return df

    def fetch_all_ticker(self):
        query = text("""
                     SELECT DISTINCT ticker FROM main_historicalstock
                     """)
        with self.connection.connect as conn:
            result = conn.execute(query)
            tickers = [row[0] for row in result.fetchall()]
        return tickers


class DataController:
    def __init__(self):
        self.df = Backend().load_close_data()
        self.backend = Backend()
        pass

    @st.cache_data
    def get_tickers(self):
        tickers = self.backend.fetch_all_ticker
        return tickers

    @st.cache_data  # to not to run the code again
    def get_dataFrame(self, ticker):
        data = self.backend.fetch_close_data(ticker_name=ticker)
        data.dropna()
        return data

    def get_30_latest_data(self, data):
        latest_data_feature = np.array(data['close'].tail(30)).reshape(1, 30)
        return latest_data_feature

    def normalise(self, latest_data_feature, feature_scalers_path):
        feature_scalers = joblib.load(feature_scalers_path)
        latest_data_feature_norm = feature_scalers[0].transform(
            latest_data_feature)
        return latest_data_feature_norm


class Predictor:  # Make prediction with TensorFlow model
    def __init__(self, model_path, x_scaler_path, y_scaler_path):
        self.predict_model = tf.keras.models.load_model(model_path)
        self.x_scaler = joblib.load(x_scaler_path)
        self.y_scaler = joblib.load(y_scaler_path)

    def get_prediction(loaded_model, X_test_norm):
        # Get prediction on the test data
        y_pred_norm = loaded_model.predict(X_test_norm)
        return y_pred_norm

    def predict_7days(self, latest_data_feature_norm):
        y_pred = get_prediction(self.predict_model, latest_data_feature_norm)
        return y_pred

    def get_signal(y_pred):
        # Calculate the change in price (percentage change)
        changes = ((y_pred[0][-1] - y_pred[0][0]) / y_pred[0][0]) * 100
        if changes >= 5:
            return "Sell", changes
        elif changes < -5:
            return "Buy", changes
        return "Hold", changes

    def get_signal_date_n_price(y_pred_denorm, df):
        # Flatten the 2D array to 1D if it's not already 1D
        # Use with cautious!!!
        y_pred_flat = y_pred_denorm.flatten()

        # Ensure future_dates has the correct number of dates (look_ahead is 7)
        look_ahead = 7
        last_date = df.index[-1]  # The last date in the historical data
        # Generate the future dates (7 days ahead, including weekends)
        future_dates = pd.date_range(
            start=last_date, periods=look_ahead + 1, freq='D')[1:]  # Exclude the start date
        # Create a pandas Series to map the predicted values to the corresponding dates
        future_prices = pd.Series(y_pred_flat, index=future_dates)

        # Find the index of the best predicted return
        best_buy_day_index = np.argmin(y_pred_flat)  # Find the index of lowest price
        best_sell_day_index = np.argmax(y_pred_flat)  # Find index of highest price
        # Get the best buy date (the date corresponding to the highest predicted return)
        best_buy_date = future_dates[best_buy_day_index]
        best_sell_date = future_dates[best_sell_day_index]
        # Get the price for the best buy date
        price_on_best_buy_day = future_prices[best_buy_date]
        price_on_best_sell_day = future_prices[best_sell_date]

        return best_buy_date, price_on_best_buy_day, best_sell_date, price_on_best_sell_day


if __name__ == "__main__":
    def predict_buying_date(ticker, model_path, feature_scalers_path, y_scaler_path):
        data = DataController.get_dataFrame(ticker)
        # Get the latest data for the selected ticker
        latest_data_feature = DataController.get_30_latest_data(data)

        # Normalize the data using the feature scaler
        data_norm = DataController.normalise(latest_data_feature, feature_scalers_path)

        # Get the prediction from the model
        y_pred = Predictor.predict_7days(model_path, data_norm)

        # Denormalize the prediction (post-prediction logic)
        y_scaler = joblib.load(y_scaler_path)
        y_pred_denorm = y_scaler.inverse_transform(y_pred)

        # Get the buying/selling signal
        # signal_n_changes = get_signal(y_pred_denorm)
        buy_date = Predictor.get_signal_date_n_price(y_pred_denorm, data)
        return buy_date
        # Paths
    vn_model_folder = os.path.join("models", "vn")
    feature_scalers_path = os.path.join(vn_model_folder,
                                        "task3_feature_scalers.pkl")
    y_scaler_path = os.path.join(vn_model_folder, "task3_y_scaler.pkl")
    model_path = os.path.join(vn_model_folder,
                            "3.1_7days_close_model_gru_ts.keras")
    buy_date = predict_buying_date("VES", model_path, feature_scalers_path, y_scaler_path)
