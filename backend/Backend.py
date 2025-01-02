import sys
import os
import pandas as pd
import numpy as np
import joblib
import tensorflow as tf
import streamlit as st
from sqlalchemy import exc, text
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
        try:
            with self.connection.connect() as conn:
                df = pd.read_sql_query(query, conn,
                                       params={"ticker_name": ticker_name})
            return df
        except exc.SQLAlchemyError as e:
            logging.error(f"Database query failed: {str(e)}")
            raise CustomException(f"Error fetch close data: {e}", sys)

    def fetch_all_ticker(self):
        query = text("""
                     SELECT DISTINCT ticker FROM main_historicalstock
                     """)
        try:
            with self.connection.connect() as conn:
                result = conn.execute(query)
                tickers = [row[0] for row in result.fetchall()]
            return tickers
        except exc.SQLAlchemyError as e:
            logging.error(f"Database query failed: {str(e)}")
            raise CustomException(f"Error fetch all ticker: {e}", sys)


class DataController:
    def __init__(self):
        self.backend = Backend()

    # @st.cache_data
    def get_tickers(self):
        tickers = self.backend.fetch_all_ticker()
        logging.info(f"ticker list: {tickers}")
        return tickers

    # @st.cache_data  # to not to run the code again
    def get_dataFrame(self, ticker):
        data = self.backend.fetch_close_data(ticker_name=ticker)
        data = data.dropna().drop_duplicates()
        return data

    def get_30_latest_data(self, data):
        latest_data_feature = np.array(data['close'].tail(30)).reshape(1, 30)
        return latest_data_feature

    def normalise(self, latest_data_feature, feature_scalers_path):
        feature_scalers = load_scaler(feature_scalers_path)
        latest_data_feature_norm = feature_scalers[0].transform(
            latest_data_feature)
        return latest_data_feature_norm


@st.cache_data
def load_scaler(scaler_path):
    return joblib.load(scaler_path)


class Predictor:  # Make prediction with TensorFlow model
    def __init__(self, model_path, x_scaler_path, y_scaler_path):
        self.predict_model = tf.keras.models.load_model(model_path)
        self.x_scaler = load_scaler(x_scaler_path)
        self.y_scaler = load_scaler(y_scaler_path)

    '''
    def get_prediction(self, loaded_model, X_test_norm):
        # Get prediction on the test data
        y_pred_norm = loaded_model.predict(X_test_norm)
        return y_pred_norm

    def predict_7days(self, latest_data_feature_norm):
        y_pred = get_prediction(self.predict_model, latest_data_feature_norm)
        return y_pred'''

    def predict_7days(self, latest_data_feature_norm):
        y_pred = self.predict_model.predict(latest_data_feature_norm)
        return y_pred

    def get_signal(self, y_pred):
        # Calculate the change in price (percentage change)
        changes = ((y_pred[0][-1] - y_pred[0][0]) / y_pred[0][0]) * 100
        if changes >= 5:
            return "Sell", changes
        elif changes < -5:
            return "Buy", changes
        return "Hold", changes

    def get_signal_date_n_price(self, y_pred_denorm, df):
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
        # Find the index of lowest price
        best_buy_day_index = np.argmin(y_pred_flat)
        # Find index of highest price
        best_sell_day_index = np.argmax(y_pred_flat)
        # Get the best buy date (the date corresponding to the highest predicted return)
        best_buy_date = future_dates[best_buy_day_index]
        best_sell_date = future_dates[best_sell_day_index]
        # Get the price for the best buy date
        price_on_best_buy_day = future_prices[best_buy_date]
        price_on_best_sell_day = future_prices[best_sell_date]

        return best_buy_date, price_on_best_buy_day, best_sell_date, price_on_best_sell_day
