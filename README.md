"dl-stock-prediction" 

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
│   ├── 1_nasdaq_analysis.ipynb
│   ├── 2_vietnam_analysis.ipynb
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
│   ├── utils/
│   │   └── db_connection.py
│   ├── Logger.py       
│   │      
│   └── main.py         
│
├── deployment/
|    ├── api.py
|    ├── app.py
|    ├── requirements.txt
|    └── Dockerfile
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
