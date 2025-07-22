import logging
from logging.handlers import RotatingFileHandler
import os

# Define the log file path
LOG_FILE = "C:/Users/Administrator/Downloads/DTN-Data-Ingestion-System/app.log"

def setup_logging():
    """
    Sets up the logging configuration for the application.
    Logs will be written to 'app.log' with a rotating file handler.
    """
    # Create a logger
    logger = logging.getLogger("dtn_ingestion_logger")
    logger.setLevel(logging.INFO) # Set the minimum logging level

    # Create a rotating file handler
    # Max 1 GB per file, keep up to 5 backup files
    handler = RotatingFileHandler(LOG_FILE, maxBytes=1024*1024*1024, backupCount=5)
    
    # Create a formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    
    # Add formatter to handler
    handler.setFormatter(formatter)
    
    # Add handler to logger
    if not logger.handlers: # Prevent adding multiple handlers if setup_logging is called multiple times
        logger.addHandler(handler)
        # Optionally, add a StreamHandler to output logs to console as well
        # console_handler = logging.StreamHandler()
        # console_handler.setFormatter(formatter)
        # logger.addHandler(console_handler)
    
    return logger

# Initialize the logger when this module is imported
logger = setup_logging()
