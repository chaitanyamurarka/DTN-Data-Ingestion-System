"""
Live Tick Data Ingestion Service

This service ingests real-time tick data from IQFeed and publishes it to Redis
for downstream processing. It also handles backfilling historical ticks on startup
and dynamically manages symbol subscriptions based on Redis configuration.
"""

from config.logging_config import logger
import json
import time
from datetime import datetime, timezone
import numpy as np
import pyiqfeed as iq
import redis
from zoneinfo import ZoneInfo
import threading

from scripts.dtn_iq_client import (
    get_iqfeed_quote_conn, 
    get_iqfeed_history_conn, 
    launch_iqfeed_service_if_needed
)
from config.config import settings


class LiveTickListener(iq.SilentQuoteListener):
    """Processes live tick data from IQFeed and publishes to Redis."""
    
    def __init__(self, name="LiveTickListener"):
        super().__init__(name)
        self.redis_client = redis.Redis.from_url(settings.REDIS_URL)
        self.source_timezone = ZoneInfo("America/New_York")
    
    def backfill_intraday_data(self, symbol: str, hist_conn: iq.HistoryConn):
        """Backfill today's raw ticks from IQFeed on startup."""
        logger.info(f"Backfilling intraday ticks for {symbol}")
        
        try:
            today_ticks = hist_conn.request_ticks_for_days(
                ticker=symbol, num_days=1, ascend=True
            )
            
            if today_ticks is None or len(today_ticks) == 0:
                logger.info(f"No intraday data to backfill for {symbol}")
                return
            
            cache_key = f"intraday_ticks:{symbol}"
            self.redis_client.delete(cache_key)
            
            pipeline = self.redis_client.pipeline()
            
            for tick in today_ticks:
                naive_dt = iq.date_us_to_datetime(tick['date'], tick['time'])
                aware_dt = naive_dt.replace(tzinfo=self.source_timezone)
                
                tick_data = {
                    "timestamp": aware_dt.timestamp(),
                    "price": float(tick['last']),
                    "volume": int(tick['last_sz']),
                }
                pipeline.rpush(cache_key, json.dumps(tick_data))
            
            pipeline.expire(cache_key, 86400)
            pipeline.execute()
            
            logger.info(f"Backfilled {len(today_ticks)} ticks for {symbol}")
            
        except iq.NoDataError:
            logger.warning(f"No tick data available for {symbol}")
        except Exception as e:
            logger.error(f"Backfill error for {symbol}: {e}", exc_info=True)
    
    def _publish_tick(self, symbol: str, price: float, volume: int):
        """Publish a tick to Redis channel and cache."""
        tick_data = {
            "price": price,
            "volume": volume,
            "timestamp": datetime.now(timezone.utc).timestamp()
        }
        
        # Real-time channel
        channel = f"live_ticks:{symbol}"
        self.redis_client.publish(channel, json.dumps(tick_data))
        
        # Cache for new clients
        cache_key = f"intraday_ticks:{symbol}"
        self.redis_client.pipeline().rpush(
            cache_key, json.dumps(tick_data)
        ).expire(cache_key, 86400).execute()
    
    def process_summary(self, summary_data: np.ndarray) -> None:
        """Handle summary messages as ticks with zero volume."""
        for summary in summary_data:
            symbol = summary['Symbol'].decode('utf-8')
            price = float(summary['Most Recent Trade'])
            
            if price > 0:
                self._publish_tick(symbol, price, 0)
    
    def process_update(self, update_data: np.ndarray) -> None:
        """Handle trade update messages."""
        for trade in update_data:
            price = float(trade['Most Recent Trade'])
            volume = int(trade['Most Recent Trade Size'])
            
            if price <= 0 or volume <= 0:
                continue
            
            symbol = trade['Symbol'].decode('utf-8')
            self._publish_tick(symbol, price, volume)


class SymbolManager:
    """Manages dynamic symbol subscriptions based on Redis configuration."""
    
    def __init__(self, redis_client, quote_conn, hist_conn, listener):
        self.redis_client = redis_client
        self.quote_conn = quote_conn
        self.hist_conn = hist_conn
        self.listener = listener
        self.watched_symbols = set()
    
    def update_symbols(self):
        """Update watched symbols from Redis configuration."""
        logger.info("Checking for symbol updates...")
        
        try:
            symbols_data_json = self.redis_client.get("dtn:ingestion:symbols")
            if not symbols_data_json:
                symbols_from_redis = set()
            else:
                symbols_data = json.loads(symbols_data_json)
                symbols_from_redis = {
                    item["symbol"] for item in symbols_data 
                    if "symbol" in item
                }
            
            to_add = symbols_from_redis - self.watched_symbols
            to_remove = self.watched_symbols - symbols_from_redis
            
            for symbol in to_add:
                self.listener.backfill_intraday_data(symbol, self.hist_conn)
                self.quote_conn.trades_watch(symbol)
                self.watched_symbols.add(symbol)
                logger.info(f"Added {symbol} to watch list")
            
            for symbol in to_remove:
                self.quote_conn.unwatch(symbol)
                self.watched_symbols.remove(symbol)
                logger.info(f"Removed {symbol} from watch list")
                
        except Exception as e:
            logger.error(f"Error updating symbols: {e}", exc_info=True)


def redis_listener(symbol_manager):
    """Listen for Redis Pub/Sub messages to trigger symbol updates."""
    pubsub = symbol_manager.redis_client.pubsub()
    pubsub.subscribe("dtn:ingestion:symbol_updates")
    
    logger.info("Subscribed to symbol updates channel")
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            symbol_manager.update_symbols()


def main():
    """Main entry point for live tick ingestion."""
    launch_iqfeed_service_if_needed()
    
    try:
        redis_client = redis.Redis.from_url(settings.REDIS_URL)
        redis_client.ping()
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection failed: {e}")
        return
    
    quote_conn = get_iqfeed_quote_conn()
    hist_conn = get_iqfeed_history_conn()
    
    if not quote_conn or not hist_conn:
        logger.error("IQFeed connections unavailable")
        return
    
    listener = LiveTickListener()
    quote_conn.add_listener(listener)
    
    symbol_manager = SymbolManager(redis_client, quote_conn, hist_conn, listener)
    
    with iq.ConnConnector([quote_conn, hist_conn]):
        symbol_manager.update_symbols()
        
        listener_thread = threading.Thread(
            target=redis_listener,
            args=(symbol_manager,),
            daemon=True
        )
        listener_thread.start()
        
        try:
            logger.info("Live tick ingestion started")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            for symbol in symbol_manager.watched_symbols:
                quote_conn.unwatch(symbol)


if __name__ == "__main__":
    main()
