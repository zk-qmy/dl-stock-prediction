import streamlit as st
import joblib
import os
from backend import charts
from backend.Backend import DataController, Predictor

os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
vn_model_folder = os.path.join("models", "vn")
feature_scalers_path = os.path.join(vn_model_folder,
                                    "task3_feature_scalers.pkl")
y_scaler_path = os.path.join(vn_model_folder, "task3_y_scaler.pkl")
model_path = os.path.join(vn_model_folder,
                          "3.1_7days_close_model_gru_ts.keras")

# Set up the title
st.title("Buying/Selling Signal Prediction")

DATA_CONTROLLER = DataController()
PREDICTOR = Predictor(model_path=model_path,
                      x_scaler_path=feature_scalers_path,
                      y_scaler_path=y_scaler_path)
tickers = DATA_CONTROLLER.get_tickers()
selected_ticker = st.selectbox("Select ticker", tickers)

selected_ticker_df = DATA_CONTROLLER.get_dataFrame(ticker=selected_ticker)
selected_ticker_latest_30days = DATA_CONTROLLER.get_30_latest_data(
    data=selected_ticker_df)

# Streamlit UI setup
data_load_status = st.empty()
data_load_status.text("Loading data....")

# Make prediction with TensorFlow model


def predict_buying_date(ticker, model_path, feature_scalers_path, y_scaler_path):
    data_controller = DataController()
    data = data_controller.get_dataFrame(ticker)

    # Get the latest data for the selected ticker
    latest_data_feature = data_controller.get_30_latest_data(data)

    # Normalize the data using the feature scaler
    data_norm = data_controller.normalise(
        latest_data_feature, feature_scalers_path)

    # Get the prediction from the model
    predictor = Predictor(model_path, feature_scalers_path, y_scaler_path)
    y_pred = predictor.predict_7days(data_norm)

    # Denormalize the prediction (post-prediction logic)
    y_scaler = joblib.load(y_scaler_path)
    y_pred_denorm = y_scaler.inverse_transform(y_pred)

    # Get the buying/selling signal
    # signal_n_changes = get_signal(y_pred_denorm)
    buy_date = Predictor.get_signal_date_n_price(y_pred_denorm=y_pred_denorm,
                                                 df=data)
    return buy_date


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
price_data = DATA_CONTROLLER.get_dataFrame(selected_ticker)
# st.write(price_data.head())
price_data.set_index('time', inplace=True)
st.line_chart(price_data['close'])
fig = charts.plot_trading_signal(price_data)
st.plotly_chart(fig)
