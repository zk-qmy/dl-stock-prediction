from src.etl.Crawler import Crawler
from src.Logger import logging


if __name__ == "__main__":
    # Crawl raw data and store in staging table
    try:
        CRAWLER = Crawler()
        CRAWLER.crawl_raw_historical_vn()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    # Overall Preprocess
    # Store to main table and backup table
