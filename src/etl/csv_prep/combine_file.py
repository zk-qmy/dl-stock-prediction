import os
import pandas as pd
from src.utils.db_connection import load_config

'''Combine all industry analysis files'''


config = load_config()
# Path to the folder containing CSV files
folder_path = config['data']["industry_analysis"]


# List all CSV files in the folder
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Combine all CSV files into a single DataFrame
combined_data = pd.concat([pd.read_csv(os.path.join(folder_path, file))
                          for file in csv_files], ignore_index=True)

# Save the combined DataFrame to a new CSV file
output_dir = "data\\toprocess\\"
# Create directory if it does not exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
# Save DataFrame as CSV
combined_data.to_csv(os.path.join(
    output_dir,
    'vn_test_selected.csv'),
    index=False)
