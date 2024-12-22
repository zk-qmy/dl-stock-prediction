from vnstock import listing_companies, financial_ratio, stock_historical_data
import os
from src.Logger import logging
from src.Exception import CustomException
import sys
import pandas as pd
from datetime import datetime
from src.utils.SQLManager import SQLManager

# TO DO: crawl the newest data + incrementally crawl and store data
# TO DO: versonning data
# TO DO: databasse tables: staging table-insert data from csv file, backuptable-store backup data, maintable-store final data(processed)


class Crawler:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def ensure_dir_exists(self, dir):
        if not os.path.exists(dir):
            logging.info(f"Creating dir: {dir}")
            os.makedirs(dir, exist_ok=True)

    def crawl_raw_financial_vn(self, input_path="financial-ratio"):
        logging.info("Enter Crawler - crawl_raw_financial_vn()")
        # Create folder
        output_path = os.path.join(self.output_dir, input_path)
        self.ensure_dir_exists(output_path)

        companies_df = listing_companies()
        company_tickers = companies_df["ticker"]
        company_stock_exchanges = companies_df["comGroupCode"]

        # Some crawlings failed due to no financial ratio in the past
        for i in range(len(company_tickers)):
            try:
                logging.info(f"Processing({i}) {company_tickers[i]}")
                logging.info(f"Start crawling raw financial: {company_tickers[i]}")
                df_financial_ratio = financial_ratio(
                    company_tickers[i], 'quarterly', True)
                logging.info("Finish crawling!")

                df_financial_ratio["ticker"] = company_tickers[i]
                df_financial_ratio["comGroupCode"] = company_stock_exchanges[i]
                logging.info(df_financial_ratio)
                # insert data to SQL database
                manager = SQLManager()
                logging.info("Start creating table in db")
                manager.create_table_from_df(df_financial_ratio, "financial")
                logging.info(f"Finish staging for {company_tickers[i]}!")
                logging.info(f"Financial output path: {output_path}")
            except Exception as e:
                logging.error(
                    f"Error processing financial data: {company_tickers[i]}: {e}")
                raise CustomException(e, sys)

    def crawl_raw_historical_vn(self, input_path="historical-stock"):
        logging.info("Enter Crawler - crawl_raw_historical_vn()")
        # Create folder
        output_path = os.path.join(self.output_dir, input_path)
        # os.makedirs(output_path, exist_ok=True)
        self.ensure_dir_exists(output_path)

        companies_df = listing_companies()
        company_tickers = companies_df["ticker"]
        company_stock_exchanges = companies_df["comGroupCode"]
        # Ensure start and end dates are in the correct format (YYYY-MM-DD)
        start_date = "1900-01-01"
        end_date = "2023-04-30"
        '''
        try:
            # Check if date format is correct
            datetime.strptime(start_date, r"%Y-%m-%d")
            datetime.strptime(end_date, r"%Y-%m-%d")

        except ValueError as e:
            logging.error(f"Invalid date format: {e}")
            raise CustomException(f"Invalid date format: {e}", sys)'''

        for i in range(len(company_tickers)):
            try:
                logging.info(f"Start crawling({i}) {company_tickers[i]}")
                logging.info(f"start_date: {start_date}, end_date: {end_date}")
                # Craw data from start date to end date
                df_stock_historical_data = stock_historical_data(
                    symbol=company_tickers[i],
                    start_date=start_date, end_date=end_date)
                logging.info("Finish crawling!")
                if df_stock_historical_data.empty:
                    logging.warning(f"No data returned for {company_tickers[i]}")
                    continue
                logging.info(df_stock_historical_data)
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

                df_stock_historical_data["ticker"] = company_tickers[i]
                df_stock_historical_data["comGroupCode"] = company_stock_exchanges[i]
                '''
                csv_file_name = os.path.join(
                    output_path,
                    f"{company_tickers[i]}-{company_stock_exchanges[i]}-History.csv")
                df_stock_historical_data.to_csv(
                    csv_file_name, encoding="utf-8-sig")'''
                # insert data to SQL database
                manager = SQLManager()
                logging.info("Start creating table in db")
                manager.create_table_from_df(
                    df_stock_historical_data, "historical-stock"
                    )
                logging.info(f"Historical output path: {output_path}")
                logging.info(
                    f"Processed historical stock: {company_tickers[i]}")
            except Exception as e:
                logging.error(
                    f"Error processing historical {company_tickers[i]}: {e}")
                raise CustomException(e, sys)


'''
if __name__ == "__main__":
    logging.info("Started Crawler")
'''
