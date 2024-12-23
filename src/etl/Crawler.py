import sys
import pandas as pd
from datetime import datetime
from vnstock import listing_companies, stock_historical_data
from src.Logger import logging
from src.Exception import CustomException
from src.etl.SQLManager import SQLManager

# TO DO: crawl the newest data + incrementally crawl and store data: store the global date(min) in the table to start crawl from that date next time
# TO DO: versoning data
# TO DO: database tables: staging table- store raw crawl data
# backup table-store backup data, main table-store final data(processed)


class Crawler:
    def __init__(self):
        print("init Crawler!")
        self.sql_manager = SQLManager()
        print("inited SQL manager from crawler!")

    def crawl_raw_historical_vn(self):
        logging.info("Enter Crawler - crawl_raw_historical_vn()")

        companies_df = listing_companies()
        company_tickers = companies_df["ticker"]
        company_stock_exchanges = companies_df["comGroupCode"]

        # Ensure start and end dates are in the correct format (YYYY-MM-DD)
        # TO DO: set default start date to 2000-01-01
        # TO DO: update the start day by getting the lastest date from metadata table
        start_date = "2000-01-01"
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

        # TO DO: create meta table
        
        # Start the loop
        for i in range(len(company_tickers)):
            try:
                # logging.info(f"Start crawling({i}) {company_tickers[i]}")
                # Craw data from start date to end date
                df_stock_historical_data = stock_historical_data(
                    symbol=company_tickers[i],
                    start_date=start_date, end_date=end_date)
                # logging.info(f"Finish crawling {company_tickers[i]}!")
                if df_stock_historical_data.empty:
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
        # TO DO: logic to get the latest_crawl date
        # TO DO: insert the latest crawl date to meta table
        # self.sql_manager.update_metadata_table(lastest_crawl_date)
        # TO DO: compare and insert data in staging to main ?
        # TO DO: backup data
        self.sql_manager.close_connection()


if __name__ == "__main__":
    try:
        CRAWLER = Crawler()
        CRAWLER.crawl_raw_historical_vn()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
