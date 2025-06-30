import logging
import json
import time
from datetime import datetime, timezone, timedelta
import numpy as np
import pyiqfeed as iq
import redis
from zoneinfo import ZoneInfo
from influxdb_client import InfluxDBClient
from datetime import datetime as dt, timezone, time as dt_time, timedelta


from dtn_iq_client import get_iqfeed_quote_conn, get_iqfeed_history_conn, launch_iqfeed_service_if_needed
from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
REDIS_URL = settings.REDIS_URL
redis_client = redis.Redis.from_url(REDIS_URL)

class DynamicLiveTickListener(iq.SilentQuoteListener):
    """Enhanced listener that dynamically manages symbols based on admin configuration"""
    
    def __init__(self, name="DynamicLiveTickListener"):
        super().__init__(name)
        self.redis_client = redis_client
        self.source_timezone = ZoneInfo("America/New_York")
        self.influx_client = InfluxDBClient(
            url=settings.INFLUX_URL,
            token=settings.INFLUX_TOKEN,
            org=settings.INFLUX_ORG
        )
        self.query_api = self.influx_client.query_api()
        self.symbol_bucket = "symbol_management"
        self.active_symbols = set()
        
    def get_active_symbols_with_config(self):
        """Get active symbols with their live config from InfluxDB"""
        flux_query = f'''
        from(bucket: "{self.symbol_bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r._measurement =~ /^symbol_/)
          |> filter(fn: (r) => r._field == "active")
          |> filter(fn: (r) => r._value == true)
          |> last()
        '''
        
        symbols_config = {}
        try:
            tables = self.query_api.query(query=flux_query)
            for table in tables:
                for record in table.records:
                    symbol = record['symbol']
                    backfill_minutes = self._get_symbol_field(symbol, 'backfill_minutes')
                    
                    # Check if live schedule is enabled
                    schedule_key = f"schedule:{symbol}_live"
                    schedule_data = self.redis_client.get(schedule_key)
                    
                    if schedule_data:
                        schedule = json.loads(schedule_data)
                        if schedule.get('enabled', False):
                            symbols_config[symbol] = {
                                'backfill_minutes': backfill_minutes or 120,
                                'config': schedule.get('config', {})
                            }
                    
        except Exception as e:
            logging.error(f"Error fetching active symbols: {e}")
        
        return symbols_config
    
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
        
        return None
    
    def backfill_intraday_data(self, symbol: str, hist_conn: iq.HistoryConn, backfill_minutes: int):
        """Backfill with configurable minutes"""
        logging.info(f"Backfilling {backfill_minutes} minutes of ticks for {symbol}...")
        try:
            # Calculate start time based on backfill_minutes
            end_time = datetime.now(self.source_timezone)
            start_time = end_time - timedelta(minutes=backfill_minutes)
            
            today_ticks = hist_conn.request_ticks_in_period(
                ticker=symbol,
                bgn_prd=start_time,
                end_prd=end_time,
                ascend=True
            )

            if today_ticks is not None and len(today_ticks) > 0:
                cache_key = f"intraday_ticks:{symbol}"
                self.redis_client.delete(cache_key)
                
                pipeline = self.redis_client.pipeline()
                
                for tick in today_ticks:
                    naive_dt = iq.date_us_to_datetime(tick['date'], tick['time'])
                    aware_dt = naive_dt.replace(tzinfo=self.source_timezone)
                    utc_timestamp = aware_dt.timestamp()

                    tick_data = {
                        "timestamp": utc_timestamp,
                        "price": float(tick['last']),
                        "volume": int(tick['last_sz']),
                    }
                    pipeline.rpush(cache_key, json.dumps(tick_data))
                
                pipeline.expire(cache_key, 86400)
                pipeline.execute()
                
                logging.info(f"Successfully backfilled {len(today_ticks)} ticks for {symbol}.")
            else:
                logging.info(f"No tick data found to backfill for {symbol}.")
        except Exception as e:
            logging.error(f"Error during tick backfill for {symbol}: {e}", exc_info=True)
    
    def _publish_tick(self, symbol: str, price: float, volume: int):
        """Publishes tick to Redis channel"""
        utc_timestamp = datetime.now(timezone.utc).timestamp()
        
        tick_data = {
            "price": price,
            "volume": volume,
            "timestamp": utc_timestamp
        }
        
        channel = f"live_ticks:{symbol}"
        self.redis_client.publish(channel, json.dumps(tick_data))

        cache_key = f"intraday_ticks:{symbol}"
        pipeline = self.redis_client.pipeline()
        pipeline.rpush(cache_key, json.dumps(tick_data))
        pipeline.execute()

    def process_summary(self, summary_data: np.ndarray) -> None:
        """Process summary messages"""
        try:
            for summary in summary_data:
                symbol = summary['Symbol'].decode('utf-8')
                if symbol in self.active_symbols:
                    price = float(summary['Most Recent Trade'])
                    if price > 0:
                        self._publish_tick(symbol, price, 0)
        except Exception as e:
            logging.error(f"Error processing SUMMARY data: {e}", exc_info=True)

    def process_update(self, update_data: np.ndarray) -> None:
        """Process trade updates"""
        try:
            for trade in update_data:
                symbol = trade['Symbol'].decode('utf-8')
                if symbol in self.active_symbols:
                    price = float(trade['Most Recent Trade'])
                    volume = int(trade['Most Recent Trade Size'])
                    
                    if price > 0 and volume > 0:
                        self._publish_tick(symbol, price, volume)
        except Exception as e:
            logging.error(f"Error processing TRADE data: {e}", exc_info=True)

def dynamic_live_ingestion():
    """Main function with dynamic symbol management"""
    launch_iqfeed_service_if_needed()
    
    quote_conn = get_iqfeed_quote_conn()
    hist_conn = get_iqfeed_history_conn()

    if not quote_conn or not hist_conn:
        logging.error("Could not get IQFeed connections. Exiting.")
        return

    listener = DynamicLiveTickListener()
    quote_conn.add_listener(listener)
    
    with iq.ConnConnector([quote_conn, hist_conn]):
        while True:
            try:
                # Get current active symbols
                symbols_config = listener.get_active_symbols_with_config()
                new_symbols = set(symbols_config.keys())
                
                # Unwatch removed symbols
                removed_symbols = listener.active_symbols - new_symbols
                for symbol in removed_symbols:
                    quote_conn.unwatch(symbol)
                    logging.info(f"Stopped watching {symbol}")
                
                # Watch new symbols
                added_symbols = new_symbols - listener.active_symbols
                for symbol in added_symbols:
                    config = symbols_config[symbol]
                    listener.backfill_intraday_data(symbol, hist_conn, config['backfill_minutes'])
                    quote_conn.trades_watch(symbol)
                    logging.info(f"Started watching {symbol} with {config['backfill_minutes']} min backfill")
                
                listener.active_symbols = new_symbols
                
                # Check for auto-stop conditions
                for symbol, config in symbols_config.items():
                    if config['config'].get('auto_stop', False):
                        # Check if market is closed
                        if not is_market_open():
                            # Remove from active symbols
                            listener.active_symbols.discard(symbol)
                            quote_conn.unwatch(symbol)
                            logging.info(f"Auto-stopped {symbol} after market close")
                
                # Sleep before next check
                time.sleep(60)  # Check every minute
                
            except KeyboardInterrupt:
                logging.info("Stopping live data ingestion.")
                break
            except Exception as e:
                logging.error(f"Error in main loop: {e}", exc_info=True)
                time.sleep(60)

def is_market_open():
    """Check if market is currently open"""
    et_zone = ZoneInfo("America/New_York")
    now_et = datetime.now(et_zone)
    
    # Market closed on weekends
    if now_et.weekday() >= 5:
        return False
    
    # Regular trading hours: 9:30 AM - 4:00 PM ET
    market_open = dt_time(9, 30)
    market_close = dt_time(16, 0)
    
    return market_open <= now_et.time() <= market_close

if __name__ == "__main__":
    logging.info("Starting Dynamic Live Tick Ingestion Service")
    dynamic_live_ingestion()