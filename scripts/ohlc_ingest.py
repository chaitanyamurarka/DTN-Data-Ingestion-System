"""
OHLC Data Ingestion Service

Ingests historical OHLC (Open, High, Low, Close) data from IQFeed and stores it
in InfluxDB. Supports multiple timeframes and dynamic symbol management via Redis.
"""

import os
import time
import threading
from datetime import datetime as dt, timezone, time as dt_time, timedelta
from typing import Optional, Dict, List
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytz
import redis
import json
import pyiqfeed as iq

from influxdb_client import InfluxDBClient, WriteOptions, WritePrecision
from influxdb_client.client.exceptions import InfluxDBError
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from urllib3.util.retry import Retry
from urllib3.exceptions import NewConnectionError

from config.logging_config import logger
from config.config import settings
from scripts.dtn_iq_client import get_iqfeed_history_conn


class InfluxConnectionManager:
    """Manages InfluxDB connections with health checks and retry logic."""
    
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = None
        self.write_api = None
        self.query_api = None
        self._health_check_interval = 60
        self._last_health_check = None
        self._is_healthy = False
        
        # Connection parameters
        self.max_retries = 3
        self.retry_delay = 5
        self.batch_size = 5000
        self.connection_timeout = 120_000
        self.write_timeout = 30_000
        
        self.initialize_connection()
    
    def initialize_connection(self):
        """Initialize or reinitialize the InfluxDB connection."""
        try:
            if self.client:
                self.client.close()
            
            retries = Retry(
                total=self.max_retries,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504]
            )
            
            self.client = InfluxDBClient(
                url=self.url,
                token=self.token,
                org=self.org,
                timeout=self.connection_timeout,
                retries=retries
            )
            
            write_options = WriteOptions(
                batch_size=self.batch_size,
                flush_interval=10_000,
                jitter_interval=2_000,
                retry_interval=5_000,
                max_retries=3,
                max_retry_delay=30_000,
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
        
        if (self._last_health_check and 
            current_time - self._last_health_check < self._health_check_interval):
            return self._is_healthy
        
        try:
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
    
    def write_with_retry(self, record: pd.DataFrame, measurement_name: str, 
                        tag_columns: List[str]):
        """Write data with retry logic and connection management."""
        for attempt in range(self.max_retries):
            try:
                if not self.ensure_connection():
                    raise ConnectionError("Cannot establish connection to InfluxDB")
                
                self.write_api.write(
                    bucket=self.bucket,
                    record=record,
                    data_frame_measurement_name=measurement_name,
                    data_frame_tag_columns=tag_columns,
                    write_precision=WritePrecision.NS
                )
                return
                
            except (NewConnectionError, ConnectionError, InfluxDBError) as e:
                logger.warning(f"Write attempt {attempt + 1} failed: {e}")
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    self.initialize_connection()
                else:
                    logger.error(f"Failed to write after {self.max_retries} attempts")
                    raise
    
    def query_with_retry(self, query: str):
        """Execute query with retry logic."""
        for attempt in range(self.max_retries):
            try:
                if not self.ensure_connection():
                    raise ConnectionError("Cannot establish connection to InfluxDB")
                
                return self.query_api.query(query=query)
                
            except Exception as e:
                logger.warning(f"Query attempt {attempt + 1} failed: {e}")
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    self.initialize_connection()
                else:
                    logger.error(f"Query failed after {self.max_retries} attempts")
                    raise
    
    def close(self):
        """Close the InfluxDB connection."""
        if self.client:
            self.client.close()
            logger.info("InfluxDB connection closed")


class OHLCDataProcessor:
    """Processes and fetches OHLC data from IQFeed."""
    
    def __init__(self, influx_manager: InfluxConnectionManager):
        self.influx_manager = influx_manager
        self.et_zone = ZoneInfo("America/New_York")
        
    def is_nasdaq_trading_hours(self, check_time_utc: Optional[dt] = None) -> bool:
        """Check if given UTC time falls within NASDAQ trading hours."""
        if check_time_utc is None:
            check_time_utc = dt.now(timezone.utc)
        
        et_time = check_time_utc.astimezone(self.et_zone)
        
        if et_time.weekday() >= 5:
            logger.info("Weekend - NASDAQ is closed")
            return False
        
        trading_start = dt_time(9, 30)
        trading_end = dt_time(16, 0)
        
        if trading_start <= et_time.time() <= trading_end:
            logger.warning(f"Current time {et_time.time()} is within NASDAQ trading hours")
            return True
        
        return False
    
    def get_last_completed_session_end_time_utc(self) -> dt:
        """Get timestamp of the end of the last fully completed trading session."""
        now_et = dt.now(self.et_zone)
        target_date_et = now_et.date()
        
        if now_et.time() < dt_time(20, 0):
            target_date_et -= timedelta(days=1)
        
        session_end_et = dt.combine(target_date_et, dt_time(20, 0), tzinfo=self.et_zone)
        return session_end_et.astimezone(timezone.utc)
    
    def get_latest_timestamp(self, symbol: str, measurement_suffix: str) -> Optional[dt]:
        """Get the latest timestamp for a symbol and measurement."""
        # Determine days to check based on timeframe
        days_to_check = {
            "1s": 7, "5s": 7, "10s": 7, "15s": 7, "30s": 7, "45s": 7,
            "1m": 180, "5m": 180, "10m": 180, "15m": 180, "30m": 180, "45m": 180,
            "1h": 180, "1d": 720
        }
        
        days = days_to_check.get(measurement_suffix, 30)
        flux_query = f'''
            from(bucket: "{self.influx_manager.bucket}")
              |> range(start: -{days}d)
              |> filter(fn: (r) => r.symbol == "{symbol}")
              |> filter(fn: (r) => r._field == "close")
              |> last()
        '''
        
        try:
            tables = self.influx_manager.query_with_retry(flux_query)
            if not tables:
                return None
            
            import re
            pattern = re.compile(f"ohlc_{re.escape(symbol)}_\\d{{8}}_{measurement_suffix}$")
            latest_time = None
            
            for table in tables:
                for record in table.records:
                    measurement = record.get_measurement()
                    if measurement and pattern.match(measurement):
                        record_time = record.get_time()
                        if record_time and (latest_time is None or record_time > latest_time):
                            latest_time = record_time
            
            if latest_time:
                logger.info(f"Found latest timestamp for {symbol}/{measurement_suffix}: {latest_time}")
                return latest_time.replace(tzinfo=timezone.utc) if latest_time.tzinfo is None else latest_time
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get latest timestamp for {symbol}/{measurement_suffix}: {e}")
            return None
    
    def format_data_for_influx(self, dtn_data: np.ndarray, symbol: str, 
                              exchange: str, tf_name: str, 
                              end_time_cutoff: Optional[dt] = None) -> Optional[pd.DataFrame]:
        """Convert IQFeed data to InfluxDB-ready DataFrame."""
        if len(dtn_data) == 0:
            return None
        
        df = pd.DataFrame(dtn_data)
        
        # Handle timestamp conversion
        if 'time' in dtn_data.dtype.names:
            timestamps_ns = df['date'].values.astype('datetime64[D]') + \
                          df['time'].values.astype('timedelta64[us]')
            df['timestamp'] = pd.to_datetime(timestamps_ns, utc=False).tz_localize('America/New_York')
        else:
            df['timestamp'] = pd.to_datetime(df['date']).dt.tz_localize('America/New_York')
        
        # Apply cutoff if specified
        if end_time_cutoff:
            df = df[df['timestamp'].dt.tz_convert('UTC') <= end_time_cutoff]
        
        if df.empty:
            return None
        
        # Create measurement name with date partitioning
        df['_measurement'] = df['timestamp'].dt.strftime(f'ohlc_{symbol}_%Y%m%d_{tf_name}')
        
        # Rename columns
        df.rename(columns={
            'open_p': 'open', 'high_p': 'high', 'low_p': 'low', 'close_p': 'close',
            'prd_vlm': 'volume', 'tot_vlm': 'total_volume'
        }, inplace=True)
        
        # Handle volume
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
    
    def fetch_and_store_history(self, symbol: str, exchange: str, 
                               hist_conn: iq.HistoryConn, timeframes: Dict):
        """Fetch and store historical data for all timeframes."""
        logger.info(f"Fetching historical data for {symbol} (Exchange: {exchange})")
        
        time.sleep(0.5)  # Rate limiting
        
        last_session_end_utc = self.get_last_completed_session_end_time_utc()
        
        for tf_name, params in timeframes.items():
            try:
                time.sleep(0.2)  # Additional rate limiting
                
                latest_timestamp = self.get_latest_timestamp(symbol, tf_name)
                dtn_data = None
                
                if params['type'] != 'd':
                    # Intraday data
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
                    # Daily data
                    days = params['days']
                    if latest_timestamp:
                        days = min(days, (dt.now(timezone.utc) - latest_timestamp).days + 1)
                    if days <= 0:
                        continue
                        
                    dtn_data = hist_conn.request_daily_data(
                        ticker=symbol, num_days=days, ascend=True
                    )
                
                if dtn_data is not None and len(dtn_data) > 0:
                    influx_df = self.format_data_for_influx(
                        dtn_data, symbol, exchange, tf_name, last_session_end_utc
                    )
                    
                    if influx_df is not None and not influx_df.empty:
                        # Write data grouped by measurement
                        grouped = influx_df.groupby('_measurement')
                        logger.info(f"Writing {len(influx_df)} points to {len(grouped)} measurements for '{tf_name}'")
                        
                        for name, group_df in grouped:
                            self.influx_manager.write_with_retry(
                                record=group_df,
                                measurement_name=name,
                                tag_columns=['symbol', 'exchange']
                            )
                        
                        logger.info(f"Write complete for {tf_name}")
                        
            except iq.exceptions.NoDataError:
                logger.info(f"No new data available for {symbol} ({tf_name})")
            except Exception as e:
                logger.error(f"Error processing {tf_name} for {symbol}: {e}", exc_info=True)
                continue


class OHLCIngestionService:
    """Main service for OHLC data ingestion."""
    
    def __init__(self):
        self.redis_client = self._init_redis()
        self.influx_manager = InfluxConnectionManager(
            url=settings.INFLUX_URL,
            token=settings.INFLUX_TOKEN,
            org=settings.INFLUX_ORG,
            bucket=settings.INFLUX_BUCKET
        )
        self.processor = OHLCDataProcessor(self.influx_manager)
        self.config = self._load_config()
        self.scheduler = None
        
    def _init_redis(self) -> redis.Redis:
        """Initialize Redis connection."""
        try:
            client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
            client.ping()
            logger.info("Successfully connected to Redis")
            return client
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Could not connect to Redis: {e}")
            raise
    
    def _load_config(self) -> Dict:
        """Load configuration from Redis or use defaults."""
        try:
            config_json = self.redis_client.get("dtn:system:config")
            if config_json:
                logger.info("Loaded system config from Redis")
                return json.loads(config_json)
        except Exception as e:
            logger.error(f"Could not load system config from Redis: {e}", exc_info=True)
        
        logger.info("Using default system config")
        return {
            "schedule_hour": 20,
            "schedule_minute": 1,
            "timeframes_to_fetch": {
                "1s": 7, "5s": 7, "10s": 7, "15s": 7, "30s": 7, "45s": 7,
                "1m": 180, "5m": 180, "10m": 180, "15m": 180, "30m": 180, "45m": 180,
                "1h": 180, "1d": 720
            }
        }
    
    def _get_timeframes(self) -> Dict:
        """Get timeframe configuration."""
        config_tf = self.config.get('timeframes_to_fetch', {})
        
        return {
            "1s":   {"interval": 1,    "type": "s", "days": config_tf.get("1s", 7)},
            "5s":   {"interval": 5,    "type": "s", "days": config_tf.get("5s", 7)},
            "10s":  {"interval": 10,   "type": "s", "days": config_tf.get("10s", 7)},
            "15s":  {"interval": 15,   "type": "s", "days": config_tf.get("15s", 7)},
            "30s":  {"interval": 30,   "type": "s", "days": config_tf.get("30s", 7)},
            "45s":  {"interval": 45,   "type": "s", "days": config_tf.get("45s", 7)},
            "1m":   {"interval": 60,   "type": "s", "days": config_tf.get("1m", 180)},
            "5m":   {"interval": 300,  "type": "s", "days": config_tf.get("5m", 180)},
            "10m":  {"interval": 600,  "type": "s", "days": config_tf.get("10m", 180)},
            "15m":  {"interval": 900,  "type": "s", "days": config_tf.get("15m", 180)},
            "30m":  {"interval": 1800, "type": "s", "days": config_tf.get("30m", 180)},
            "45m":  {"interval": 2700, "type": "s", "days": config_tf.get("45m", 180)},
            "1h":   {"interval": 3600, "type": "s", "days": config_tf.get("1h", 180)},
            "1d":   {"interval": 1,    "type": "d", "days": config_tf.get("1d", 720)}
        }
    
    def _get_symbols_from_redis(self) -> Dict[str, List[str]]:
        """Fetch and parse symbols from Redis."""
        symbols_json = self.redis_client.get("dtn:ingestion:symbols")
        
        if not symbols_json:
            logger.warning("No symbols found in Redis")
            return {}
        
        try:
            symbols_data = json.loads(symbols_json)
            if not isinstance(symbols_data, list):
                logger.error("Symbols data is not a list")
                return {}
            
            # Group symbols by exchange
            symbols_by_exchange = {}
            for item in symbols_data:
                symbol = item.get("symbol")
                exchange = item.get("exchange")
                if symbol and exchange:
                    if exchange not in symbols_by_exchange:
                        symbols_by_exchange[exchange] = []
                    symbols_by_exchange[exchange].append(symbol)
            
            return symbols_by_exchange
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding symbols from Redis: {e}")
            return {}
    
    def daily_update(self, symbols: List[str], exchange: str):
        """Perform daily update for given symbols."""
        if self.processor.is_nasdaq_trading_hours():
            logger.warning("Aborting daily update: operation not permitted during trading hours")
            return
        
        logger.info(f"Starting daily update for {len(symbols)} symbols on {exchange}")
        
        if not self.influx_manager.ensure_connection():
            logger.error("Cannot connect to InfluxDB. Aborting daily update")
            return
        
        hist_conn = get_iqfeed_history_conn()
        if hist_conn is None:
            logger.error("Could not get IQFeed connection. Aborting daily update")
            return
        
        timeframes = self._get_timeframes()
        
        with iq.ConnConnector([hist_conn]):
            for i, symbol in enumerate(symbols):
                try:
                    logger.info(f"Processing symbol {i+1}/{len(symbols)}: {symbol}")
                    self.processor.fetch_and_store_history(
                        symbol, exchange, hist_conn, timeframes
                    )
                except Exception as e:
                    logger.error(f"Failed to process {symbol}: {e}")
                    continue
        
        logger.info("Daily update process finished")
    
    def process_all_symbols(self):
        """Process all symbols from Redis."""
        logger.info("Fetching symbols from Redis for OHLC update")
        
        symbols_by_exchange = self._get_symbols_from_redis()
        if not symbols_by_exchange:
            logger.warning("No valid symbols found")
            return
        
        for exchange, symbols in symbols_by_exchange.items():
            logger.info(f"Processing {len(symbols)} symbols for exchange: {exchange}")
            self.daily_update(symbols, exchange)
    
    def _handle_symbol_update(self, message):
        """Handle symbol update message from Redis."""
        if message['type'] == 'message':
            logger.info(f"Received symbol update message: {message['data']}")
            self.process_all_symbols()
    
    def _handle_config_update(self, message):
        """Handle config update message from Redis."""
        if message['type'] == 'message':
            logger.info(f"Received config update message: {message['data']}")
            self.config = self._load_config()
            
            if self.scheduler:
                new_hour = self.config.get('schedule_hour', 20)
                new_minute = self.config.get('schedule_minute', 1)
                
                try:
                    self.scheduler.reschedule_job(
                        'daily_update_job',
                        trigger=CronTrigger(
                            hour=new_hour,
                            minute=new_minute,
                            second=0,
                            timezone=pytz.timezone('America/New_York')
                        )
                    )
                    logger.info(f"Rescheduled daily update job to {new_hour:02d}:{new_minute:02d} ET")
                except Exception as e:
                    logger.error(f"Failed to reschedule job: {e}", exc_info=True)
    
    def run(self):
        """Start the OHLC ingestion service."""
        logger.info("Starting OHLC Ingestion System...")
        
        # Initial run
        logger.info("Running initial symbol update...")
        self.process_all_symbols()
        
        # Initialize scheduler
        self.scheduler = BlockingScheduler(timezone="America/New_York")
        
        schedule_hour = self.config.get('schedule_hour', 20)
        schedule_minute = self.config.get('schedule_minute', 1)
        
        self.scheduler.add_job(
            self.process_all_symbols,
            trigger=CronTrigger(
                hour=schedule_hour,
                minute=schedule_minute,
                second=0,
                timezone=pytz.timezone('America/New_York')
            ),
            id="daily_update_job",
            name="Daily Historical Market Data Ingestion"
        )
        
        # Setup Redis Pub/Sub listeners
        symbol_pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
        symbol_pubsub.subscribe("dtn:ingestion:symbol_updates")
        symbol_thread = threading.Thread(
            target=lambda: [self._handle_symbol_update(msg) for msg in symbol_pubsub.listen()],
            daemon=True
        )
        symbol_thread.start()
        
        config_pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
        config_pubsub.subscribe("dtn:system:config_updates")
        config_thread = threading.Thread(
            target=lambda: [self._handle_config_update(msg) for msg in config_pubsub.listen()],
            daemon=True
        )
        config_thread.start()
        
        try:
            logger.info("Scheduler and listeners started. Press Ctrl+C to exit...")
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down...")
        finally:
            self.influx_manager.close()
            self.redis_client.close()
            logger.info("Shutdown complete")


def main():
    """Main entry point for OHLC ingestion."""
    service = OHLCIngestionService()
    service.run()


if __name__ == '__main__':
    main()