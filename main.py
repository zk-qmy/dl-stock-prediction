from src.etl.Crawler import Crawler
import os
# from src.utils.db_connection import SQLConnection

if __name__ == "__main__":
    # Define individual path components
    folder_name = "data"
    subfolder_name = "raw"

    # Join them together into a single path
    destination_path = os.path.join(folder_name, subfolder_name)

    # Instantiate the Crawler class and pass destination_path as output_dir
    CRAWLER = Crawler(destination_path)

    # Call the crawl_raw_historical_vn method with the destination path
    CRAWLER.crawl_raw_historical_vn(input_path=destination_path)

    # connection = SQLConnection().get_db_connection()
