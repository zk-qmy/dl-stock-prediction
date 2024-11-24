import streamlit as st
import pandas as pd
import joblib
import os
import numpy as np
import tensorflow as tf

os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

# Paths
feature_scalers_path = "src\\models\\vn\\task3_feature_scalers.pkl"
y_scaler_path = "src\\models\\vn\\task3_y_scaler.pkl"
model_path = r"src\models\vn\3.1_7days_close_model_gru_ts.keras"
data_folder_path = "data\\raw\\data-vn-20230228\\data-vn-20230228\\stock-historical-data"

# Set up the title
st.title("Buying/Selling Signal Prediction")


@st.cache_data
def get_tickers(folder_path):
    tickers = []  # Insert list of stock here
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            ticker_name = "-".join(file_name.split("-")[:-1])
            tickers.append(ticker_name)
    return tickers


tickers = get_tickers(data_folder_path)
selected_ticker = st.selectbox("Select ticker", tickers)


@st.cache_data  # to not to run the code again
def get_30_latest_data(ticker):
    data_file_path = os.path.join(data_folder_path,
                                  selected_ticker + "-History.csv")
    data = pd.read_csv(data_file_path)
    data.drop(columns="Unnamed: 0", inplace=True)
    data.drop(columns='TradingDate', inplace=True)
    data.dropna()
    latest_data_feature = np.array(data['Close'].tail(30)).reshape(1, 30)
    return latest_data_feature


def normalise(latest_data_feature, feature_scalers_path):
    feature_scalers = joblib.load(feature_scalers_path)
    latest_data_feature_norm = feature_scalers[0].transform(
        latest_data_feature)
    return latest_data_feature_norm

# Make prediction with TensorFlow model


def get_prediction(loaded_model, X_test_norm):
    # Get prediction on the test data
    y_pred_norm = loaded_model.predict(X_test_norm)
    return y_pred_norm


def predict_7days(model_path, latest_data_feature_norm):
    model = tf.keras.models.load_model(model_path)
    y_pred = get_prediction(model, latest_data_feature_norm)
    return y_pred


def get_signal(y_pred):
    # Calculate the change in price (percentage change)
    changes = ((y_pred[0][-1] - y_pred[0][0]) / y_pred[0][0]) * 100
    if changes >= 5:
        return "Sell", changes
    elif changes < -5:
        return "Buy", changes
    return "Hold", changes


def predict_signal(ticker, model_path, feature_scalers_path, y_scaler_path):
    # Get the latest data for the selected ticker
    latest_data_feature = get_30_latest_data(ticker)

    # Normalize the data using the feature scaler
    data_norm = normalise(latest_data_feature, feature_scalers_path)

    # Get the prediction from the model
    y_pred = predict_7days(model_path, data_norm)

    # Denormalize the prediction (post-prediction logic)
    y_scaler = joblib.load(y_scaler_path)
    y_pred_denorm = y_scaler.inverse_transform(y_pred)

    # Get the buying/selling signal
    signal_n_changes = get_signal(y_pred_denorm)
    return signal_n_changes


# Streamlit UI setup
data_load_status = st.empty()
data_load_status.text("Loading data....")
output_data = predict_signal(
    selected_ticker, model_path, feature_scalers_path, y_scaler_path)

# Display prediction
if output_data is None:
    data_load_status.text("Failed to load data!")
    st.write("This stock does not have data to predict!")
else:
    data_load_status.empty()  # clear the loading status
    st.success("Predicted successfully!")
    st.write(f"Signal: {output_data[0]}")
    st.write(f"Close price changes in 7 days: {output_data[1]}")
