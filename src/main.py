# import pandas as pd
from utils.db_connection import load_config, get_db_engine
'''Encounter Wildcard imports when trying to
from utils.db_connection import * '''
from etl.data_to_sql import insert_to_sql, get_data
import os

config = load_config()
engine = get_db_engine(config)
# Path to the folder containing CSV files
folder_path = config['data']['vn_230228_path']  # Change to  a test path in 'data' field for test 
db_name = config["database"]["dbname"]
table_name = config["tables"]["testvn"]  # Change to a test table in 'tables' field for test DB 

insert_to_sql(folder_path, engine, table_name)
with engine.connect() as connection:
    df = get_data(connection, db_name, table_name)
print(df)


# Specify the directory
directory = 'data\\toprocess'

# Create directory if it does not exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Save DataFrame as CSV
df.to_csv(os.path.join(directory, 'vn_selected.csv'), index=False)


# TRAIN MODEL
# Get data samples with window_size to predict the value on label_colID column
'''
get_data_samples(df, window_size, feature_col_ID, label_colID, feature_num)
return X_data, y_data

# Split the samples to train/val/test
split_data(X_data, y_data, test_size=0.2, val_size=0.2))
return X_train, X_val, X_test, y_train, y_val, y_test

reshape_data(X_train, X_val, X_test, dim_size) (optional) 
return X_train, X_val, X_test

# Normalise data
min_max_scale(train_set, val_set, test_set) # handle differnet part of this code 
return train_norm, val_norm, test_norm
# build model architecture

# plot training and validation performance
plot_performance(history)
plot_compare_pred_real(y_pred, y_real)'''