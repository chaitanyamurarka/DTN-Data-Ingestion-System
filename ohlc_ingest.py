import os
import logging
import time
from datetime import datetime as dt, timezone, time as dt_time, timedelta
import pytz
import re
import redis
import json

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import pyiqfeed as iq
from dtn_iq_client import get_iqfeed_history_conn
from config import settings

# --- Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Redis for schedule management
redis_client = redis.from_url(settings.REDIS_URL)

class DynamicOHLCIngestor:
    def __init__(self):
        self.influx_client = InfluxDBClient(
            url=settings.INFLUX_URL, 
            token=settings.INFLUX_TOKEN, 
            org=settings.INFLUX_ORG, 
            timeout=90_000
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.influx_client.query_api()
        self.symbol_bucket = "symbol_management"
        
    def get_active_symbols(self):
        """Fetch unique active symbols from InfluxDB"""
        flux_query = f'''
        from(bucket: "{self.symbol_bucket}")
        |> range(start: -30d)
        |> filter(fn: (r) => r._measurement =~ /^symbol_/)
        |> filter(fn: (r) => r._field == "active")
        |> filter(fn: (r) => r._value == true)
        |> last()
        '''
        
        symbols_map = {}
        try:
            tables = self.query_api.query(query=flux_query)
            for table in tables:
                for record in table.records:
                    symbol = record['symbol']
                    # Only add the symbol if it hasn't been added yet
                    if symbol not in symbols_map:
                        symbols_map[symbol] = {
                            'symbol': symbol,
                            'exchange': record['exchange'],
                            'historical_days': self._get_symbol_field(symbol, 'historical_days')
                        }
        except Exception as e:
            logging.error(f"Error fetching active symbols: {e}")
        
        return list(symbols_map.values())
    
    def _get_symbol_field(self, symbol, field):
        """Get specific field value for a symbol"""
        flux_query = f'''
        from(bucket: "{self.symbol_bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r.symbol == "{symbol}")
          |> filter(fn: (r) => r._field == "{field}")
          |> last()
        '''
        
        try:
            tables = self.query_api.query(query=flux_query)
            if tables and tables[0].records:
                return tables[0].records[0].get_value()
        except:
            pass
        
        return 30  # Default historical days
    
    def get_schedule_config(self, symbol):
        """Get schedule configuration from Redis"""
        key = f"schedule:{symbol}_historical"
        config_data = redis_client.get(key)
        
        if config_data:
            config = json.loads(config_data)
            return config.get('config', {})
        
        # Default config
        return {
            'intervals': ['1s', '5s', '10s', '15s', '30s', '45s', 
                         '1m', '5m', '10m', '15m', '30m', '45m', 
                         '1h', '1d']
        }
    
    def ingest_symbol(self, symbol_info, hist_conn):
        """Ingest historical data for a single symbol"""
        symbol = symbol_info['symbol']
        exchange = symbol_info['exchange']
        historical_days = symbol_info.get('historical_days', 30)
        
        config = self.get_schedule_config(symbol)
        intervals = config.get('intervals', ['1m', '5m', '15m', '30m', '1h', '1d'])
        
        logging.info(f"Processing {symbol} - {exchange} for {historical_days} days")
        
        last_session_end_utc = get_last_completed_session_end_time_utc()
        
        timeframes_to_fetch = {
            "1s":   {"interval": 1,    "type": "s", "days": min(7, historical_days)},
            "5s":   {"interval": 5,    "type": "s", "days": min(7, historical_days)},
            "10s":  {"interval": 10,   "type": "s", "days": min(7, historical_days)},
            "15s":  {"interval": 15,   "type": "s", "days": min(7, historical_days)},
            "30s":  {"interval": 30,   "type": "s", "days": min(7, historical_days)},
            "45s":  {"interval": 45,   "type": "s", "days": min(7, historical_days)},
            "1m":   {"interval": 60,   "type": "s", "days": historical_days},
            "5m":   {"interval": 300,  "type": "s", "days": historical_days},
            "10m":  {"interval": 600,  "type": "s", "days": historical_days},
            "15m":  {"interval": 900,  "type": "s", "days": historical_days},
            "30m":  {"interval": 1800, "type": "s", "days": historical_days},
            "45m":  {"interval": 2700, "type": "s", "days": historical_days},
            "1h":   {"interval": 3600, "type": "s", "days": historical_days},
            "1d":   {"interval": 1,    "type": "d", "days": historical_days}
        }
        
        # Only fetch configured intervals
        for tf_name in intervals:
            if tf_name not in timeframes_to_fetch:
                continue
                
            params = timeframes_to_fetch[tf_name]
            
            try:
                latest_timestamp = self.get_latest_timestamp(symbol, tf_name)
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
                    influx_df = self.format_data_for_influx(dtn_data, symbol, exchange, tf_name, last_session_end_utc)
                    if influx_df is not None and not influx_df.empty:
                        grouped_by_measurement = influx_df.groupby('_measurement')
                        logging.info(f"Writing {len(influx_df)} points for {symbol} - {tf_name}")
                        
                        for name, group_df in grouped_by_measurement:
                            self.write_api.write(
                                bucket=settings.INFLUX_BUCKET,
                                record=group_df,
                                data_frame_measurement_name=name,
                                data_frame_tag_columns=['symbol', 'exchange']
                            )
                            
            except Exception as e:
                logging.error(f"Error fetching {tf_name} for {symbol}: {e}", exc_info=True)
    
    def get_latest_timestamp(self, symbol: str, measurement_suffix: str) -> dt | None:
        # Similar to original implementation
        sanitized_symbol = re.escape(symbol)
        measurement_regex = f"^ohlc_{sanitized_symbol}_\\d{{8}}_{measurement_suffix}$"
        flux_query = f'''
            from(bucket: "{settings.INFLUX_BUCKET}")
              |> range(start: 0)
              |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{symbol}")
              |> last() |> keep(columns: ["_time"])
        '''
        try:
            tables = self.query_api.query(query=flux_query)
            if not tables or not tables[0].records: 
                return None
            latest_time = tables[0].records[0].get_time()
            return latest_time.replace(tzinfo=timezone.utc) if latest_time.tzinfo is None else latest_time
        except Exception: 
            return None
    
    def format_data_for_influx(self, dtn_data, symbol, exchange, tf_name, end_time_utc_cutoff):
        # Similar to original implementation
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
    
    def run_ingestion(self):
        """Main ingestion process"""
        if is_nasdaq_trading_hours():
            logging.warning("Aborting: operation not permitted during trading hours.")
            return
        
        logging.info("--- Starting Dynamic Symbol Ingestion ---")
        
        hist_conn = get_iqfeed_history_conn()
        if hist_conn is None:
            logging.error("Could not get IQFeed connection.")
            return
        
        with iq.ConnConnector([hist_conn]):
            symbols = self.get_active_symbols()
            logging.info(f"Found {len(symbols)} active symbols to process")
            
            for symbol_info in symbols:
                try:
                    self.ingest_symbol(symbol_info, hist_conn)
                    
                    # Update last ingestion timestamp
                    self._update_last_ingestion(symbol_info['symbol'])
                    
                except Exception as e:
                    logging.error(f"Error processing {symbol_info['symbol']}: {e}")
                    continue
        
        logging.info("--- Dynamic Ingestion Complete ---")
    
    def _update_last_ingestion(self, symbol):
        """Update last ingestion timestamp in InfluxDB"""
        try:
            # Find the measurement for this symbol
            flux_query = f'''
            from(bucket: "{self.symbol_bucket}")
              |> range(start: -30d)
              |> filter(fn: (r) => r.symbol == "{symbol}")
              |> filter(fn: (r) => r._measurement =~ /^symbol_/)
              |> last()
            '''
            
            tables = self.query_api.query(flux_query)
            if tables and tables[0].records:
                record = tables[0].records[0]
                measurement = record.get_measurement()
                
                # Write update
                point = Point(measurement) \
                    .tag("symbol", symbol) \
                    .tag("exchange", record['exchange']) \
                    .tag("security_type", record['security_type']) \
                    .field("last_ingestion", dt.now(timezone.utc).isoformat()) \
                    .time(dt.now(timezone.utc), WritePrecision.NS)
                
                self.write_api.write(bucket=self.symbol_bucket, record=point)
        except Exception as e:
            logging.error(f"Error updating last ingestion for {symbol}: {e}")

def is_nasdaq_trading_hours(check_time_utc: dt | None = None) -> bool:
    """Check if within NASDAQ trading hours"""
    et_zone = ZoneInfo("America/New_York")
    if check_time_utc is None:
        check_time_utc = dt.now(timezone.utc)
    et_time = check_time_utc.astimezone(et_zone)
    if et_time.weekday() >= 5:
        return False
    trading_start = dt_time(9, 30)
    trading_end = dt_time(16, 0)
    return trading_start <= et_time.time() <= trading_end

def get_last_completed_session_end_time_utc() -> dt:
    """Get last completed session end time"""
    et_zone = ZoneInfo("America/New_York")
    now_et = dt.now(et_zone)
    target_date_et = now_et.date()
    if now_et.time() < dt_time(20, 0):
        target_date_et -= timedelta(days=1)
    session_end_et = dt.combine(target_date_et, dt_time(20, 0), tzinfo=et_zone)
    return session_end_et.astimezone(timezone.utc)

def load_schedules_from_redis():
    """Load all enabled schedules from Redis and configure the scheduler"""
    scheduler = BlockingScheduler(timezone="America/New_York")
    
    # Get all schedule keys
    schedule_pattern = "schedule:*_historical"
    
    for key in redis_client.scan_iter(match=schedule_pattern):
        try:
            schedule_data = json.loads(redis_client.get(key))
            
            if not schedule_data.get('enabled', False):
                continue
            
            cron_expression = schedule_data['cron_expression']
            cron_parts = cron_expression.split()
            
            trigger = CronTrigger(
                minute=cron_parts[0],
                hour=cron_parts[1],
                day=cron_parts[2],
                month=cron_parts[3],
                day_of_week=cron_parts[4],
                timezone=pytz.timezone('America/New_York')
            )
            
            scheduler.add_job(
                DynamicOHLCIngestor().run_ingestion,
                trigger=trigger,
                id=f"ingestion_{schedule_data['id']}",
                name=f"Historical ingestion for {schedule_data['symbol']}"
            )
            
            logging.info(f"Scheduled job for {schedule_data['symbol']} with cron: {cron_expression}")
            
        except Exception as e:
            logging.error(f"Error loading schedule from {key}: {e}")
    
    return scheduler

if __name__ == '__main__':
    logging.info("Starting Dynamic OHLC Ingestion Service")
    
    # Run initial ingestion
    ingestor = DynamicOHLCIngestor()
    ingestor.run_ingestion()
    
    # Load and start scheduler
    scheduler = load_schedules_from_redis()
    
    if scheduler.get_jobs():
        logging.info(f"Scheduler started with {len(scheduler.get_jobs())} jobs")
        logging.info("Press Ctrl+C to exit.")
        
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logging.info("Scheduler stopped. Shutting down...")
    else:
        logging.info("No scheduled jobs found. Service will exit.")
    
    if ingestor.influx_client:
        ingestor.influx_client.close()
        logging.info("InfluxDB client closed.")