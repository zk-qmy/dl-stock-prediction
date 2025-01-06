import sys
import pandas as pd
from datetime import datetime
from vnstock import listing_companies, stock_historical_data
from src.Logger import logging
from src.Exception import CustomException
from src.etl.SQLManager import SQLManager
import requests


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

    def get_company_tickers(self, start_ticker="all"):
        companies_df = listing_companies()
        full_ticker_list = companies_df['ticker'].to_list()
        full_comp_exchanges_list = companies_df["comGroupCode"].to_list()
        # Combine the two lists into a list of tuples
        full_ticker_exchange_tuples = list(
            zip(full_ticker_list, full_comp_exchanges_list)
            )
        if start_ticker == "all":
            return full_ticker_exchange_tuples
        if start_ticker != "all" and start_ticker not in full_ticker_list:
            raise CustomException(f"Ticker {start_ticker} not found in the list", sys)

        # Find the index of start_ticker
        start_index = full_ticker_list.index(start_ticker)
        # Slice from index of start_ticker onwards
        partial_ticker_list = full_ticker_list[start_index:]
        partial_comp_exchanges_list = full_comp_exchanges_list[start_index:]
        partial_ticker_exchanges_tuples = list(
            zip(partial_ticker_list, partial_comp_exchanges_list)
        )
        logging.info(f"Ticker start index: {start_index},"
                     + f" New ticker list: {partial_ticker_exchanges_tuples}")
        return partial_ticker_exchanges_tuples

    def crawl_raw_historical_vn(self,
                                staging_table_name, meta_table_name,
                                start_ticker="all"):
        logging.info("Enter Crawler - crawl_raw_historical_vn()")

        company_tickers = self.get_company_tickers(start_ticker=start_ticker)

        # Ensure start and end dates are in the correct format (YYYY-MM-DD)
        # Get start date: the latest_crawl date - 3days. IF SCHEDULE THE CRALER
        # TO CRAWL EVERY MONDAY THEN DO NOT NEED TO CRAWL DATA FROM 3 DAYS AHEAD
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
            df_stock_historical_data = None  # Init dataframe
            try:
                # Craw data from start date to end date
                df_stock_historical_data = stock_historical_data(
                    symbol=company_tickers[i][0],
                    start_date=start_date, end_date=end_date)
                # logging.info(df_stock_historical_data)
                # logging.info(f"Finish crawling {company_tickers[i]}!")
                if df_stock_historical_data is None:
                    logging.warning(f"Dataframe {company_tickers[i][0]} is None!")
                    df_stock_historical_data = pd.DataFrame()
                if df_stock_historical_data.empty:
                    logging.warning(f"No data for {company_tickers[i]}")
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

                df_stock_historical_data["code"] = company_tickers[i][1]
                # insert data to SQL database
                self.sql_manager.run_group1_insert_raw_to_staging(
                    df=df_stock_historical_data,
                    table_name=staging_table_name)
                logging.info(
                    f"Processed historical stock: {company_tickers[i]}")
            except requests.exceptions.HTTPError as e:  # catch error raise by library
                if "400" in str(e) and "invalid symbol" in str(e).lower():
                    logging.warning(f"Skipping ticker {company_tickers[i][0]} due to BAD_REQUEST error.")
                else:
                    logging.error(f"HTTP error occured: {e}")
                continue  # skip this ticker and move to the next one
            except UnboundLocalError as ue:
                logging.warning("Failed to retrieve data due to an invalid API response or other issue."
                                + f" when processing {company_tickers[i][0]}: {ue}")
                continue
            except Exception as e:
                logging.error(
                    f"Error processing historical {company_tickers[i]}: {e}")
                logging.info(df_stock_historical_data)
                raise CustomException(e, sys)
        logging.info("-----FINISH CRAWLING AND INSERT HISTORICAL STOCK!-----")
        return end_date


if __name__ == "__main__":
    try:
        CRAWLER = Crawler()
        CRAWLER.crawl_raw_historical_vn(start_ticker="VES",
                                        staging_table_name="test_staging",
                                        meta_table_name="test_meta")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
