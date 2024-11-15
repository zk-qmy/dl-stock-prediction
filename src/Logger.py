import logging
from datetime import datetime
import os


log_file = f"{datetime.now().strftime(r'%d%m%Y_%H%M%S')}.log"
log_path = os.path.join(os.getcwd(), "logs", log_file)
os.makedirs(log_path, exist_ok=True)

log_file_path = os.path.join(log_path, log_file)

logging.basicConfig(
    filename=log_file_path,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s]",
    level=logging.INFO,

)


# Start logging if script is executed as main
if __name__ == "__main__":
    logging.info("Started logging!")
