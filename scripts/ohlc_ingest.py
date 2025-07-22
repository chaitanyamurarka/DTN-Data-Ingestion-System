import os
from config.logging_config import logger
import time
from datetime import datetime as dt, timezone, time as dt_time, timedelta
import pytz
import re
import threading
from typing import Optional, Dict, List
import redis
import json

from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from urllib3.util.retry import Retry
from urllib3.exceptions import NewConnectionError, MaxRetryError

# Local imports
import pyiqfeed as iq
from scripts.dtn_iq_client import get_iqfeed_history_conn
from config.config import settings

# --- Configuration ---
load_dotenv()
logger.info("OHLC Ingestion System started.")

# InfluxDB Configuration
INFLUX_URL = settings.INFLUX_URL
INFLUX_TOKEN = settings.INFLUX_TOKEN
INFLUX_ORG = settings.INFLUX_ORG
INFLUX_BUCKET = settings.INFLUX_BUCKET

# Connection retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_SIZE = 5000  # points per write
CONNECTION_TIMEOUT = 120_000  # milliseconds
WRITE_TIMEOUT = 30_000  # milliseconds

class InfluxConnectionManager:
    """Manages InfluxDB connections with health checks and retry logic."""
    
    def __init__(self):
        self.client = None
        self.write_api = None
        self.query_api = None
        self._last_health_check = None
        self._health_check_interval = 60  # seconds
        self._is_healthy = False
        self.initialize_connection()
    
    def initialize_connection(self):
        """Initialize or reinitialize the InfluxDB connection."""
        try:
            if self.client:
                self.client.close()
            
            # Create client with custom retry configuration
            retries = Retry(
                total=MAX_RETRIES,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504]
            )
            
            self.client = InfluxDBClient(
                url=INFLUX_URL,
                token=INFLUX_TOKEN,
                org=INFLUX_ORG,
                timeout=CONNECTION_TIMEOUT,
                retries=retries
            )
            
            # Use batch writing with error callbacks
            write_options = WriteOptions(
                batch_size=BATCH_SIZE,
                flush_interval=10_000,  # milliseconds
                jitter_interval=2_000,  # milliseconds
                retry_interval=5_000,   # milliseconds
                max_retries=3,
                max_retry_delay=30_000,  # milliseconds
                exponential_base=2
            )
            
            self.write_api = self.client.write_api(write_options=write_options)
            self.query_api = self.client.query_api()
            self._is_healthy = self.check_health()
            
            logger.info("InfluxDB connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize InfluxDB connection: {e}")
            self._is_healthy = False
    
    def check_health(self) -> bool:
        """Check if InfluxDB is healthy and accessible."""
        current_time = time.time()
        
        # Skip health check if we've checked recently
        if (self._last_health_check and 
            current_time - self._last_health_check < self._health_check_interval):
            return self._is_healthy
        
        try:
            # Perform a simple ping to check connectivity
            self.client.ping()
            self._is_healthy = True
            self._last_health_check = current_time
            return True
        except Exception as e:
            logger.error(f"InfluxDB health check failed: {e}")
            self._is_healthy = False
            self._last_health_check = current_time
            return False
    
    def ensure_connection(self) -> bool:
        """Ensure we have a healthy connection, reconnecting if necessary."""
        if not self.check_health():
            logger.warning("InfluxDB connection unhealthy, attempting to reconnect...")
            self.initialize_connection()
            return self._is_healthy
        return True
    
    def write_with_retry(self, bucket: str, record: pd.DataFrame, 
                        measurement_name: str, tag_columns: List[str]):
        """Write data with retry logic and connection management."""
        for attempt in range(MAX_RETRIES):
            try:
                if not self.ensure_connection():
                    raise ConnectionError("Cannot establish connection to InfluxDB")
                
                # Write data
                self.write_api.write(
                    bucket=bucket,
                    record=record,
                    data_frame_measurement_name=measurement_name,
                    data_frame_tag_columns=tag_columns,
                    write_precision=WritePrecision.NS
                )
                
                # If successful, break out of retry loop
                return
                
            except (NewConnectionError, ConnectionError, InfluxDBError) as e:
                logger.warning(f"Write attempt {attempt + 1} failed: {e}")
                
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                    self.initialize_connection()  # Try to reconnect
                else:
                    logger.error(f"Failed to write after {MAX_RETRIES} attempts")
                    raise
    
    def query_with_retry(self, query: str):
        """Execute query with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                if not self.ensure_connection():
                    raise ConnectionError("Cannot establish connection to InfluxDB")
                
                return self.query_api.query(query=query)
                
            except Exception as e:
                logger.warning(f"Query attempt {attempt + 1} failed: {e}")
                
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                    self.initialize_connection()
                else:
                    logger.error(f"Query failed after {MAX_RETRIES} attempts")
                    raise
    
    def close(self):
        """Close the InfluxDB connection."""
        if self.client:
            self.client.close()
            logger.info("InfluxDB connection closed")

# Global connection manager
influx_manager = InfluxConnectionManager()

def is_nasdaq_trading_hours(check_time_utc: Optional[dt] = None) -> bool:
    """Checks if a given UTC time falls within NASDAQ trading hours."""
    et_zone = ZoneInfo("America/New_York")
    
    if check_time_utc is None:
        check_time_utc = dt.now(timezone.utc)
    
    et_time = check_time_utc.astimezone(et_zone)
    
    if et_time.weekday() >= 5:
        logger.info("Skipping operation: It's the weekend, NASDAQ is closed.")
        return False
    
    trading_start = dt_time(9, 30)
    trading_end = dt_time(16, 0)
    
    if trading_start <= et_time.time() <= trading_end:
        logger.warning(
            f"Current time {et_time.time()} is within NASDAQ trading hours. Deferring operations.")
        return True
    
    logger.info(f"Current time {et_time.time()} is outside NASDAQ trading hours.")
    return False

def get_last_completed_session_end_time_utc() -> dt:
    """Determines the timestamp of the end of the last fully completed trading session."""
    et_zone = ZoneInfo("America/New_York")
    now_et = dt.now(et_zone)
    
    target_date_et = now_et.date()
    
    if now_et.time() < dt_time(20, 0):
        target_date_et -= timedelta(days=1)
    
    session_end_et = dt.combine(target_date_et, dt_time(20, 0), tzinfo=et_zone)
    
    return session_end_et.astimezone(timezone.utc)

def get_latest_timestamp(symbol: str, measurement_suffix: str) -> Optional[dt]:
    """Get the latest timestamp for a symbol and measurement."""
    # Try the most efficient method first
    result = get_latest_timestamp_by_timeframe(symbol, measurement_suffix)
    if result:
        return result
    
    # Fallback to simpler method if needed
    return get_latest_timestamp_simple(symbol, measurement_suffix)


def get_latest_timestamp_simple(symbol: str, measurement_suffix: str) -> Optional[dt]:
    """Simpler approach that avoids schema collision."""
    # Query without keeping multiple columns of different types
    flux_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -180d)
          |> filter(fn: (r) => r.symbol == "{symbol}")
          |> filter(fn: (r) => r._field == "close")
          |> last()
    '''
    
    try:
        tables = influx_manager.query_with_retry(flux_query)
        if not tables:
            return None
        
        # Filter by measurement pattern in Python
        pattern = re.compile(f"ohlc_{re.escape(symbol)}_\\d{{8}}_{measurement_suffix}$")
        latest_time = None
        latest_measurement = None
        
        for table in tables:
            for record in table.records:
                measurement = record.get_measurement()
                if measurement and pattern.match(measurement):
                    record_time = record.get_time()
                    if record_time and (latest_time is None or record_time > latest_time):
                        latest_time = record_time
                        latest_measurement = measurement
        
        if latest_time:
            logger.info(f"Found latest data timestamp as {latest_time} for {symbol} in measurement: {latest_measurement}")
            return latest_time.replace(tzinfo=timezone.utc) if latest_time.tzinfo is None else latest_time
        
        logger.info(f"No data found for {symbol} with suffix {measurement_suffix}")
        return None
        
    except Exception as e:
        logger.error(f"Simple method also failed for {symbol}/{measurement_suffix}: {e}")
        return None


def get_latest_timestamp_by_timeframe(symbol: str, measurement_suffix: str) -> Optional[dt]:
    """Alternative method that constructs specific measurement names."""
    # Based on the timeframe suffix, determine how many days back to look
    days_to_check = {
        "1s": 7, "5s": 7, "10s": 7, "15s": 7, "30s": 7, "45s": 7,
        "1m": 180, "5m": 180, "10m": 180, "15m": 180, "30m": 180, "45m": 180,
        "1h": 180, "1d": 720
    }
    
    days = days_to_check.get(measurement_suffix, 30)
    end_date = dt.now(timezone.utc)
    start_date = end_date - timedelta(days=days)
    
    # Try to find data starting from the most recent date
    current_date = end_date
    while current_date >= start_date:
        measurement_name = f"ohlc_{symbol}_{current_date.strftime('%Y%m%d')}_{measurement_suffix}"
        
        flux_query = f'''
            from(bucket: "{INFLUX_BUCKET}")
              |> range(start: -1d, stop: now())
              |> filter(fn: (r) => r._measurement == "{measurement_name}")
              |> filter(fn: (r) => r.symbol == "{symbol}")
              |> filter(fn: (r) => r._field == "close")
              |> last()
        '''
        
        try:
            tables = influx_manager.query_with_retry(flux_query)
            if tables and tables[0].records:
                latest_time = tables[0].records[0].get_time()
                logger.info(f"Found latest data timestamp as {latest_time} for {symbol} in measurement: {measurement_name}")
                return latest_time.replace(tzinfo=timezone.utc) if latest_time.tzinfo is None else latest_time
        except:
            pass  # Try next date
        
        current_date -= timedelta(days=1)
    
    logger.info(f"Missing data for {symbol} with suffix {measurement_suffix} in the last {days} days")
    return None

def format_data_for_influx(
    dtn_data: np.ndarray,
    symbol: str,
    exchange: str,
    tf_name: str,
    end_time_utc_cutoff: Optional[dt] = None
) -> Optional[pd.DataFrame]:
    """Converts NumPy array from pyiqfeed to a Pandas DataFrame ready for InfluxDB."""
    if len(dtn_data) == 0:
        return None
    
    df = pd.DataFrame(dtn_data)
    
    has_time_field = 'time' in dtn_data.dtype.names
    
    if has_time_field:
        timestamps_ns = df['date'].values.astype('datetime64[D]') + df['time'].values.astype('timedelta64[us]')
        df['timestamp'] = pd.to_datetime(timestamps_ns, utc=False).tz_localize('America/New_York')
    else:
        df['timestamp'] = pd.to_datetime(df['date']).dt.tz_localize('America/New_York')
    
    if end_time_utc_cutoff:
        df = df[df['timestamp'].dt.tz_convert('UTC') <= end_time_utc_cutoff]
    
    if df.empty:
        return None
    
    df['_measurement'] = df['timestamp'].dt.strftime(f'ohlc_{symbol}_%Y%m%d_{tf_name}')
    
    df.rename(columns={
        'open_p': 'open', 'high_p': 'high', 'low_p': 'low', 'close_p': 'close',
        'prd_vlm': 'volume', 'tot_vlm': 'total_volume'
    }, inplace=True)
    
    if 'volume' not in df.columns and 'total_volume' in df.columns:
        df['volume'] = df['total_volume']
    if 'volume' not in df.columns:
        df['volume'] = 0
    
    df['volume'] = df['volume'].astype('int64')
    df['symbol'] = symbol
    df['exchange'] = exchange
    
    df.set_index('timestamp', inplace=True)
    
    final_cols = ['open', 'high', 'low', 'close', 'volume', 'symbol', 'exchange', '_measurement']
    return df[[col for col in final_cols if col in df.columns]]

def fetch_and_store_history(symbol: str, exchange: str, hist_conn: iq.HistoryConn):
    """Fetch and store historical data with improved error handling."""
    logger.info(f"Fetching historical data for {symbol} (Exchange: {exchange})")
    
    # Add delay between symbols to avoid overwhelming the server
    time.sleep(0.5)
    
    last_session_end_utc = get_last_completed_session_end_time_utc()
    
    timeframes_to_fetch = {
        "1s":   {"interval": 1,    "type": "s", "days": 7},
        "5s":   {"interval": 5,    "type": "s", "days": 7},
        "10s":  {"interval": 10,   "type": "s", "days": 7},
        "15s":  {"interval": 15,   "type": "s", "days": 7},
        "30s":  {"interval": 30,   "type": "s", "days": 7},
        "45s":  {"interval": 45,   "type": "s", "days": 7},
        "1m":   {"interval": 60,   "type": "s", "days": 180},
        "5m":   {"interval": 300,  "type": "s", "days": 180},
        "10m":  {"interval": 600,  "type": "s", "days": 180},
        "15m":  {"interval": 900,  "type": "s", "days": 180},
        "30m":  {"interval": 1800, "type": "s", "days": 180},
        "45m":  {"interval": 2700, "type": "s", "days": 180},
        "1h":   {"interval": 3600, "type": "s", "days": 180},
        "1d":   {"interval": 1,    "type": "d", "days": 720}
    }
    
    for tf_name, params in timeframes_to_fetch.items():
        try:
            # Add small delay between timeframes
            time.sleep(0.2)
            
            latest_timestamp = get_latest_timestamp(symbol, tf_name)
            dtn_data = None
            
            if params['type'] != 'd':
                start_dt = latest_timestamp or (last_session_end_utc - timedelta(days=params['days']))
                if start_dt >= last_session_end_utc:
                    continue
                dtn_data = hist_conn.request_bars_in_period(
                    ticker=symbol,
                    interval_len=params['interval'],
                    interval_type=params['type'],
                    bgn_prd=start_dt,
                    end_prd=last_session_end_utc,
                    ascend=True
                )
            else:
                days = params['days'] if not latest_timestamp else (dt.now(timezone.utc) - latest_timestamp).days + 1
                if days <= 0:
                    continue
                dtn_data = hist_conn.request_daily_data(ticker=symbol, num_days=days, ascend=True)
            
            if dtn_data is not None and len(dtn_data) > 0:
                influx_df = format_data_for_influx(dtn_data, symbol, exchange, tf_name, last_session_end_utc)
                
                if influx_df is not None and not influx_df.empty:
                    grouped_by_measurement = influx_df.groupby('_measurement')
                    logger.info(f"Writing {len(influx_df)} points to {len(grouped_by_measurement)} measurements for '{tf_name}'...")
                    
                    for name, group_df in grouped_by_measurement:
                        influx_manager.write_with_retry(
                            bucket=INFLUX_BUCKET,
                            record=group_df,
                            measurement_name=name,
                            tag_columns=['symbol', 'exchange']
                        )
                    
                    logger.info(f"Write complete for {tf_name}")
        except iq.exceptions.NoDataError:
            # <<< FIX: Gracefully handle NoDataError >>>
            logger.info(f"No new data available for {symbol} ({tf_name}). Database is up to date.")
        except Exception as e:
            logger.error(f"Error processing {tf_name} for {symbol}: {e}", exc_info=True)
            # Continue with next timeframe instead of failing completely
            continue

def daily_update(symbols_to_update: List[str], exchange: str):
    """Performs the daily update with improved error handling."""
    logger.info("--- Checking conditions for Daily Update Process ---")
    
    if is_nasdaq_trading_hours():
        logger.warning("Aborting daily update: operation not permitted during trading hours.")
        return
    
    logger.info("--- Starting Daily Update Process ---")
    
    # Check InfluxDB health before starting
    if not influx_manager.ensure_connection():
        logger.error("Cannot connect to InfluxDB. Aborting daily update.")
        return
    
    hist_conn = get_iqfeed_history_conn()
    if hist_conn is None:
        logger.error("Could not get IQFeed connection. Aborting daily update.")
        return
    
    with iq.ConnConnector([hist_conn]):
        for i, symbol in enumerate(symbols_to_update):
            try:
                logger.info(f"Processing symbol {i+1}/{len(symbols_to_update)}: {symbol}")
                fetch_and_store_history(symbol, exchange, hist_conn)
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
                # Continue with next symbol
                continue
    
    logger.info("--- Daily Update Process Finished ---")

def process_symbols_from_redis():
    """Fetches symbols from Redis and triggers daily_update."""
    logger.info("--- Fetching symbols from Redis for OHLC update ---")
    
    try:
        REDIS_URL = settings.REDIS_URL
        r = redis.Redis.from_url(REDIS_URL)
        r.ping()
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}. Aborting OHLC update.")
        return
    
    symbols_data_json = r.get("dtn:ingestion:symbols")
    
    if not symbols_data_json:
        logger.warning("No symbols found in Redis. Aborting OHLC update.")
        return
    
    try:
        symbols_data = json.loads(symbols_data_json)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding symbols from Redis: {e}")
        return
    
    if not isinstance(symbols_data, list):
        logger.error("Symbols data from Redis is not a list.")
        return
    
    # Group symbols by exchange
    symbols_by_exchange: Dict[str, List[str]] = {}
    for item in symbols_data:
        symbol = item.get("symbol")
        exchange = item.get("exchange")
        if symbol and exchange:
            if exchange not in symbols_by_exchange:
                symbols_by_exchange[exchange] = []
            symbols_by_exchange[exchange].append(symbol)
    
    if not symbols_by_exchange:
        logger.warning("No valid symbols found in Redis.")
        return
    
    for exchange, symbols_to_update in symbols_by_exchange.items():
        logger.info(f"Processing {len(symbols_to_update)} symbols for exchange: {exchange}")
        daily_update(symbols_to_update, exchange)

def scheduled_daily_update():
    """Wrapper function for the scheduler."""
    logger.info("--- Triggering Scheduled Daily OHLC Update ---")
    process_symbols_from_redis()

def redis_pubsub_listener_ohlc():
    """Listens for Redis Pub/Sub messages."""
    try:
        REDIS_URL = settings.REDIS_URL
        r = redis.Redis.from_url(REDIS_URL)
        r.ping()
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Could not connect to Redis for Pub/Sub: {e}")
        return
    
    pubsub = r.pubsub()
    pubsub.subscribe("dtn:ingestion:symbol_updates")
    logger.info("OHLC Pub/Sub listener subscribed to 'dtn:ingestion:symbol_updates'")
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            logger.info(f"Received Pub/Sub message: {message['data']}")
            process_symbols_from_redis()

if __name__ == '__main__':
    try:
        logger.info("Starting OHLC Ingestion System...")
        
        # Initial run
        logger.info("Running initial symbol update...")
        process_symbols_from_redis()
        
        # Initialize scheduler
        logger.info("Initializing scheduler...")
        scheduler = BlockingScheduler(timezone="America/New_York")
        
        scheduler.add_job(
            scheduled_daily_update,
            trigger=CronTrigger(
                hour=20,
                minute=1,
                second=0,
                timezone=pytz.timezone('America/New_York')
            ),
            name="Daily Historical Market Data Ingestion"
        )
        
        # Start Redis Pub/Sub listener
        pubsub_thread = threading.Thread(
            target=redis_pubsub_listener_ohlc,
            daemon=True
        )
        pubsub_thread.start()
        
        logger.info("Scheduler started. Press Ctrl+C to exit...")
        scheduler.start()
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
    finally:
        influx_manager.close()
        logger.info("Shutdown complete.")