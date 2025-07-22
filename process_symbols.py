import zipfile
import os
import pandas as pd
import redis
import glob
import requests # Import the requests library
import shutil
from config.logging_config import logger
from config.config import settings

# Configuration
ZIP_FILE_URL = "https://github.com/chaitanyamurarka/DTN-IQFeed-Symbol-Downloader/raw/refs/heads/main/dtn_symbols/by_exchange.zip"
LOCAL_ZIP_PATH = "by_exchange.zip" # Local path to save the downloaded file
EXTRACT_DIR = "dtn_symbols_extracted"
TARGET_EXCHANGES = ["NYSE", "CME", "NASDAQ", "EUREX"]

# Redis connection
try:


    REDIS_URL = settings.REDIS_URL
    r = redis.Redis.from_url(REDIS_URL)
    r.ping()
    logger.info("Successfully connected to Redis!")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis: {e}")
    logger.error("Please ensure Redis server is running and accessible.")
    exit()

def download_file(url, destination):
    """Downloads a file from a URL to a local destination."""
    logger.info(f"Downloading file from {url} to {destination}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        with open(destination, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info("Download complete.")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        return False

def extract_zip(zip_path, extract_to):
    """Extracts a zip file to a specified directory."""
    if not os.path.exists(zip_path):
        logger.error(f"Error: Local zip file not found at {zip_path}")
        return False
    
    logger.info(f"Extracting {zip_path} to {extract_to}...")
    # Ensure the extraction directory exists and is empty
    if os.path.exists(extract_to):
        shutil.rmtree(extract_to)
    os.makedirs(extract_to)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    logger.info("Extraction complete.")
    return True

def process_and_store_symbols(extracted_dir, redis_client, target_exchanges):
    """
    Processes extracted CSVs, filters by exchange, and stores symbols in Redis
    using the columns present in the file.
    """
    processed_count = 0
    # The extracted structure is nested, e.g., .../dtn_symbols_extracted/dtn_symbols/by_exchange/
    base_path = os.path.join(extracted_dir, "dtn_symbols", "by_exchange")

    logger.info(f"Searching for exchanges in: {base_path}")

    for exchange_name in target_exchanges:
        exchange_path = os.path.join(base_path, exchange_name)
        if not os.path.isdir(exchange_path):
            logger.warning(f"Warning: Exchange directory not found for {exchange_name}")
            continue

        logger.info(f"Processing symbols for exchange: {exchange_name}")
        csv_files = glob.glob(os.path.join(exchange_path, "*.csv"))

        if not csv_files:
            logger.warning(f"No CSV files found for exchange {exchange_name}")
            continue

        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)

                # Check for the actual column names from your sample
                if 'symbol' not in df.columns or 'exchange' not in df.columns or 'securityType' not in df.columns:
                    logger.warning(f"Skipping {csv_file}: Missing one of the required columns ('symbol', 'exchange', 'securityType').")
                    continue

                # Filter the DataFrame to ensure we only process symbols for the target exchange
                df_filtered = df[df['exchange'] == exchange_name]

                if df_filtered.empty:
                    continue
                
                # Group by the securityType to create the Redis key
                grouped = df_filtered.groupby('securityType')

                for sec_type, group_df in grouped:
                    redis_key = f"symbols:{exchange_name}:{sec_type}"
                    redis_client.set(redis_key, group_df.to_json(orient='records'))
                    processed_count += len(group_df)

            except Exception as e:
                logger.error(f"Error processing {csv_file}: {e}")

    logger.info(f"Finished processing. Total symbols stored in Redis: {processed_count}")

def main():
    # 1. Download the file from the URL
    if not download_file(ZIP_FILE_URL, LOCAL_ZIP_PATH):
        return # Exit if download fails

    try:
        # 2. Extract the locally downloaded zip file
        if extract_zip(LOCAL_ZIP_PATH, EXTRACT_DIR):
            # 3. Process and store symbols in Redis
            process_and_store_symbols(EXTRACT_DIR, r, TARGET_EXCHANGES)
    finally:
        # 4. Clean up downloaded and extracted files
        if os.path.exists(LOCAL_ZIP_PATH):
            os.remove(LOCAL_ZIP_PATH)
            print(f"Cleaned up downloaded zip: {LOCAL_ZIP_PATH}")
        if os.path.exists(EXTRACT_DIR):
            shutil.rmtree(EXTRACT_DIR)
            print(f"Cleaned up extracted directory: {EXTRACT_DIR}")

if __name__ == "__main__":
    main()