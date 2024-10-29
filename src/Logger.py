import logging
from datetime import datetime
import os
import inspect


class Logger:
    def __init__(self, log_file_name, log_dir="logs/logMessages/"):
        self.log_dir = log_dir
        # create new log file if not exist
        os.makedirs(log_dir, exist_ok=True)
        # log file path
        # toSQLWarning
        self.log_filename = f"{log_file_name}_{datetime.now().strftime('%%d%%m%%Y_%%H%%M%%S')}.log"
        self.log_path = os.path.join(log_dir, self.log_filename)
        # configure the logging
        logging.basicConfig(
            filename=self.log_path,
            filemode="a",  # use "a" to append to the log file
            format="[%(asctime)s - %(levelname)s - %(message)s]",
            level=logging.WARNING,
            handlers=[
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _get_function_name(self):
        """Get the name of the calling function."""
        return inspect.currentframe().f_back.f_code.co_name

    def log_warning(self, message):
        function_name = self._get_function_name()
        self.logger.warning(f"{function_name}: {message}")

    def log_error(self, message):
        function_name = self._get_function_name()
        self.logger.error(f"{function_name}: {message}")

    def log_info(self, message):
        function_name = self._get_function_name()
        self.logger.info(f"{function_name}: {message}")
    # Usage
    '''
    # Create an instance of the Logger
    log = Logger(log_file_name="toSQLWarning")

    # Log a warning message using the log_warning method
    log.log_warning("This is a warning message.")

    '''
