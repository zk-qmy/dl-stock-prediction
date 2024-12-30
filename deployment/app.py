import streamlit as st
import pandas as pd
import joblib
import os
import numpy as np
import tensorflow as tf
import charts

os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

# Paths
vn_model_folder = os.path.join("models", "vn")
feature_scalers_path = os.path.join(vn_model_folder,
                                    "task3_feature_scalers.pkl")
y_scaler_path = os.path.join(vn_model_folder, "task3_y_scaler.pkl")
model_path = os.path.join(vn_model_folder,
                          "3.1_7days_close_model_gru_ts.keras")
# data_folder_path = "data\\raw\\data-vn-20230228\\data-vn-20230228\\stock-historical-data"

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
def get_dataFrame(ticker):
    data_file_path = os.path.join(data_folder_path,
                                  selected_ticker + "-History.csv")
    data = pd.read_csv(data_file_path)
    data.drop(columns="Unnamed: 0", inplace=True)
    # data.drop(columns='TradingDate', inplace=True)
    data["TradingDate"] = pd.to_datetime(data["TradingDate"])
    data.set_index("TradingDate", inplace=True)
    data.dropna()
    return data


def get_30_latest_data(data):
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


def predict_buying_date(ticker, model_path, feature_scalers_path, y_scaler_path):
    data = get_dataFrame(ticker)
    # Get the latest data for the selected ticker
    latest_data_feature = get_30_latest_data(data)

    # Normalize the data using the feature scaler
    data_norm = normalise(latest_data_feature, feature_scalers_path)

    # Get the prediction from the model
    y_pred = predict_7days(model_path, data_norm)

    # Denormalize the prediction (post-prediction logic)
    y_scaler = joblib.load(y_scaler_path)
    y_pred_denorm = y_scaler.inverse_transform(y_pred)

    # Get the buying/selling signal
    # signal_n_changes = get_signal(y_pred_denorm)
    buy_date = get_signal_date_n_price(y_pred_denorm, data)
    return buy_date


# Streamlit UI setup
data_load_status = st.empty()
data_load_status.text("Loading data....")
output_data = predict_buying_date(
    selected_ticker, model_path, feature_scalers_path, y_scaler_path)

# Display prediction
if output_data is None:
    data_load_status.text("Failed to load data!")
    st.write("This stock does not have data to predict!")
else:
    data_load_status.empty()  # clear the loading status
    st.success("Predicted successfully!")
# Display Buy and Sell details
st.subheader("Here is the buying/selling signal for the next week:")
col1, col2 = st.columns(2)
with col1:
    st.markdown("#### 🟢 **Buy Information**")
    st.markdown(f"**Buy Date**: _{output_data[0].strftime('%Y-%m-%d')}_")
    st.markdown(f"**Buy Price**: _${output_data[1]:,.2f}_")

with col2:
    st.markdown("#### 🔴 **Sell Information**")
    st.markdown(f"**Sell Date**: _{output_data[2].strftime('%Y-%m-%d')}_")
    st.markdown(f"**Sell Price**: _${output_data[3]:,.2f}_")

# Display a simple price trend chart
st.markdown("---")
st.write("### 📉 Price Trend Around Buy/Sell Dates")
price_data = get_dataFrame(selected_ticker)
# st.write(price_data.head())
st.line_chart(price_data['Close'])
fig = charts.plot_trading_signal(price_data)
st.plotly_chart(fig)
