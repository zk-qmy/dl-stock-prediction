import sys
import pandas as pd
from datetime import datetime
from vnstock import listing_companies, stock_historical_data
from src.Logger import logging
from src.Exception import CustomException
from src.etl.SQLManager import SQLManager
# TO DO: crawl start from a certain ticker
# idea: create get_company_ticker function to return all tickers
#  for the crawl_start_from: adjust so that
'''my_list = ["a", "b", "c"]

# Find the index of "b" and slice from there
start_index = my_list.index("b")  # Find the index of "b"
result = my_list[start_index:]  # Slice from index of "b" onwards

print(result)  # Output: ['b', 'c']
'''


class Crawler:
    def __init__(self):
        print("init Crawler!")
        self.sql_manager = SQLManager()
        print("inited SQL manager from crawler!")

    def get_last_crawl_date(self, meta_table_name):
        default_date = "2000-01-01"
        try:
            last_crawl_date = self.sql_manager.fetch_last_crawl_date(
                meta_table_name=meta_table_name)
            if last_crawl_date:
                return last_crawl_date
            return default_date
        except Exception as e:
            raise CustomException(f"Error fetching last crawl date: {e}", sys)

    def crawl_raw_historical_vn(self, staging_table_name, meta_table_name):
        logging.info("Enter Crawler - crawl_raw_historical_vn()")

        companies_df = listing_companies()
        company_tickers = companies_df["ticker"]
        company_stock_exchanges = companies_df["comGroupCode"]

        # Ensure start and end dates are in the correct format (YYYY-MM-DD)
        # Get start date: the latest_crawl date - 3days. IF SCHEDULE THE CRALER
        # TO CRAWL EVERY MONDAY THEN DO NOT NEED TO CRAWL DATA FROM 3 DAYS AHEAD
        # Calculate the previous 3rd day
        latest_date = datetime.strptime(
            self.get_last_crawl_date(meta_table_name=meta_table_name),
            "%Y-%m-%d")
        # previous_3rd_day = latest_date - timedelta(days=3)
        start_date = latest_date.strftime('%Y-%m-%d')

        today = datetime.today()
        end_date = today.strftime('%Y-%m-%d')  # Convert to string 'YYYY-MM-DD'
        logging.info(f"Crawl data from {start_date} to {end_date}.")

        # Start the loop
        for i in range(len(company_tickers)):
            try:
                # Craw data from start date to end date
                df_stock_historical_data = stock_historical_data(
                    symbol=company_tickers[i],
                    start_date=start_date, end_date=end_date)
                # logging.info(f"Finish crawling {company_tickers[i]}!")
                if df_stock_historical_data.empty or df_stock_historical_data is None:
                    logging.warning(f"No data for {company_tickers[i]}")
                    continue
                if ("date" in df_stock_historical_data.columns):
                    df_stock_historical_data["date"] = pd.to_datetime(
                        df_stock_historical_data["date"],
                        errors="coerce"
                    )
                elif ("tradingDate" in df_stock_historical_data.columns):
                    df_stock_historical_data["tradingDate"] = pd.to_datetime(
                        df_stock_historical_data["tradingDate"],
                        errors="coerce"
                    )
                elif ("time" in df_stock_historical_data.columns):
                    df_stock_historical_data["time"] = pd.to_datetime(
                        df_stock_historical_data["time"],
                        errors="coerce"
                    )

                df_stock_historical_data["code"] = company_stock_exchanges[i]
                # insert data to SQL database
                self.sql_manager.run_group1_insert_raw_to_staging(
                    df=df_stock_historical_data,
                    table_name=staging_table_name)
                logging.info(
                    f"Processed historical stock: {company_tickers[i]}")
            except Exception as e:
                logging.error(
                    f"Error processing historical {company_tickers[i]}: {e}")
                raise CustomException(e, sys)
        return end_date


if __name__ == "__main__":
    try:
        CRAWLER = Crawler()
        CRAWLER.crawl_raw_historical_vn()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
