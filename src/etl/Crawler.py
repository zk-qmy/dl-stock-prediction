import sys
import pandas as pd
from datetime import datetime, timedelta
from vnstock import listing_companies, stock_historical_data
from src.Logger import logging
from src.Exception import CustomException
from src.etl.SQLManager import SQLManager

# TO DO: crawl the newest data + incrementally crawl and store data: store the global date(min) in the table to start crawl from that date next time
# TO DO: versoning data
# TO DO: table- store raw crawl data
# TO DO: logic to insert data from staging to main table
# TO DO: main table
# backup table-store backup data, main table-store final data(processed)
# TO DO: create a trnasaction, if fail at any step then rolllback

class Crawler:
    def __init__(self):
        print("init Crawler!")
        self.sql_manager = SQLManager()
        print("inited SQL manager from crawler!")

    def get_last_crawl_date(self):
        default_date = "2000-01-01"
        try:
            last_crawl_date = self.sql_manager.fetch_latest_crawl_date()
            if last_crawl_date:
                return last_crawl_date
            return default_date
        except Exception as e:
            logging.error(f"Error fetching last crawl date: {e}")
            return

    def crawl_raw_historical_vn(self):
        logging.info("Enter Crawler - crawl_raw_historical_vn()")

        companies_df = listing_companies()
        company_tickers = companies_df["ticker"]
        company_stock_exchanges = companies_df["comGroupCode"]

        # Ensure start and end dates are in the correct format (YYYY-MM-DD)
        # Get start date: the latest_crawl date - 3days. IF SCHEDULE THE CRALER
        # TO CRAWL EVERY MONDAY THEN DO NOT NEED TO CRAWL DATA FROM 3 DAYS AHEAD
        # Calculate the previous 3rd day
        latest_date = datetime.strptime(self.get_last_crawl_date(), "%Y-%m-%d")
        # previous_3rd_day = latest_date - timedelta(days=3)
        start_date = latest_date.strftime('%Y-%m-%d')

        today = datetime.today()
        end_date = today.strftime('%Y-%m-%d')  # Convert to string 'YYYY-MM-DD'
        logging.info(f"Crawl data from {start_date} to {end_date}.")
        '''
        try:
            # Check if date format is correct
            datetime.strptime(start_date, r"%Y-%m-%d")
            datetime.strptime(end_date, r"%Y-%m-%d")

        except ValueError as e:
            logging.error(f"Invalid date format: {e}")
            raise CustomException(f"Invalid date format: {e}", sys)'''

        # STAGING TABLE
        self.sql_manager.create_table_from_df(, table_name="staging")
        self.sql_manager.insert_data_from_df(,table_name="staging")
        # compare with main table
        # upsert to main table
        # create/update meta table
        # create/update backup table
        
        # Create meta table
        self.sql_manager.create_metadata_table()
        # Start the loop
        for i in range(len(company_tickers)):
            try:
                # logging.info(f"Start crawling({i}) {company_tickers[i]}")
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
                table_name = "historicalStock"
                self.sql_manager.create_table_from_df(df_stock_historical_data,
                                                      table_name=table_name)
                self.sql_manager.insert_data_from_df(df_stock_historical_data,
                                                     table_name=table_name)
                logging.info(
                    f"Processed historical stock: {company_tickers[i]}")
            except Exception as e:
                logging.error(
                    f"Error processing historical {company_tickers[i]}: {e}")
                raise CustomException(e, sys)
        # Insert the latest crawl date to meta table
        self.sql_manager.update_metadata_table(end_date)
        # TO DO: compare and insert data in staging to main ?
        # TO DO: backup data
        # Close connection
        self.sql_manager.close_connection()


if __name__ == "__main__":
    try:
        CRAWLER = Crawler()
        CRAWLER.crawl_raw_historical_vn()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
