# Stock Trading Signal Prediction 📈

This project focuses on predicting stock prices and trading signals using deep learning models on two datasets: Nasdaq and Vietnam stock market data (VN dataset). The project applies LSTM, GRU, BiGRU, and CNN models for time-series forecasting and signal prediction.

## Features
* ✅ Stock Price Prediction (LSTM, GRU, BiGRU)
* ✅ Trading Signal Prediction (Buy/Sell)
* ✅ Time Series Cross-Validation
* ✅ Integration with MySQL for Data Storage
* ✅ Automated Preprocessing & Feature Engineering
* ✅ Web App display predicted Buy/Sell signal using Streamlit


# Tentative Project Structure
```
dl-stock-prediction/
│
├── data/                  
│   ├── nasdaq/           
│   │   ├── historical_data.csv
│   │   └── ...          
│   ├── vietnam/          
│   │   ├── historical_data.csv
│   │   └── ...          
│   └── processed/        
│
├── notebooks/             
│   ├── Task1-1-price-next-day-nasdag.ipynb
│   ├── Task1-2-price-next-kth-day-nasdag.ipynb
│   └── ...
│
├── src/                  
│   ├── __init__.py        
│   ├── data/     
│   │   ├── train_nasdaq.py 
│   │   ├── train_vietnam.py 
│   │   └── trading_signals.py          
│   │     
│   ├── preprocessing/
|   |	├── load_data.py
|   |   ├── prepare_data.py   
|   |   ├── preprocess.py   
│   │   └── data_to_sql.py   
│   ├── models/            
│   ├── evaluation/        
│   │   └── evaluate.py    
│   └── utils/
|       ├── Logger.py 
│       └── Exception.py              
│
├── app.py
├── main.py
|
├── requirements.txt       
├── README.md              
├── .gitignore             
└── LICENSE                
```
#Setup
## 1. Streamlit
On terminal
```
pip install streamlit
```
Navigate to the project folder
```
.\venv\Scripts\activate.bat
```
run the app.py
```
streamlit run deployment app.py
```

## 📜 Acknowledgments
This project uses the **Vnstock3** library for stock data crawling.  
- **Library:** [Vnstock3](https://github.com/thinh-vu/vnstock)  
- **Author:** Thinh Vu  
- **License:** Personal use only, non-commercial (© 2024 Thinh Vu)  

⚠ **Note:** This project is for **educational and research purposes only**. Commercial use of Vnstock3 is prohibited without written permission from the author.
Special thanks to the contributors of this library for making financial data easily accessible!

