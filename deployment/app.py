import streamlit as st
import pandas as pd
import joblib
# or any library you used to save your model

'''
G:\dl-stock-prediction\venv\Scripts\activate.bat
streamlit run deployment/app.py'''

# Load your trained model (adjust the path accordingly)
# model = joblib.load('../src/models/your_model.pkl')

# Set up the title
st.title("Stock Prediction Dashboard")

# Sidebar for user inputs
st.sidebar.header("User Input")

# Add input fields
input_value = st.sidebar.number_input("Enter a value")

# Predict button
if st.sidebar.button("Predict"):
    # Make a prediction
    prediction = model.predict([[input_value]])  # Adjust based on your model input
    st.write(f"Prediction: {prediction[0]}")


# Set up the title of the app
st.title("Stock Prediction Dashboard")

# Example DataFrame (replace this with your actual DataFrame)
data = {
    'Date': ['2023-01-01', '2023-01-02', '2023-01-03'],
    'Stock Price': [150, 152, 153],
    'Volume': [1000, 1100, 1200]
}

df = pd.DataFrame(data)

# Display the DataFrame in the app
st.subheader("Stock Data")
st.dataframe(df)  # This will render an interactive table
