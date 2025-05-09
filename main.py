from src.etl.Crawler import Crawler
from src.etl.SQLManager import SQLManager
from src.Logger import logging
from src.Exception import CustomException


if __name__ == "__main__":
    # Determine tables' name
    # The staging table will be created dynamically
    staging_table_name = "staging_historicalStock"
    main_table_name = "main_historicalStock"
    backup_table_name = "backup_historicalStock"
    meta_table_name = "metadata_historicalStock"

    '''
    staging_table_name = "test_staging"
    main_table_name = "test_main"
    backup_table_name = "test_backup"
    meta_table_name = "test_meta"
    '''

    MANAGER = SQLManager()
    CRAWLER = Crawler()
    # CRAWL DATA AND STORE IN STAGING TABLE
    try:
        MANAGER.create_metadata_table(
                meta_table_name=meta_table_name)
        to_update_last_crawl_date = CRAWLER.crawl_raw_historical_vn(
            staging_table_name=staging_table_name,
            meta_table_name=meta_table_name,
            start_ticker="KSS")  # remove this param to crawl all company
        print("Done group 1: Crawled and stored in staging table!")

    # STORE DATA TO DATABASE
        # Create main and backup table when crawl on the first time
        with MANAGER.connection.connect() as connection:
            MANAGER.create_table_like(
                connection, main_table_name, staging_table_name)
            MANAGER.create_table_like(
                connection, backup_table_name, main_table_name)
        # Store to main table and backup table
        MANAGER.run_group2_insert_staging_to_main_n_backup(
            staging_table_name, main_table_name, backup_table_name)
        print("Done group 2: Created main and backup version!")
        # Update meta table + clear staging
        MANAGER.run_group3_update_meta_n_clear_staging(
            to_update_last_crawl_date, meta_table_name, staging_table_name)
        print("Done group 3: Processed data to database!!!!!!")
    except CustomException as e:
        logging.error(e)
