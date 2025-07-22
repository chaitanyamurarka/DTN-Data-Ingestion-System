from config.logging_config import logger
import json
import time
from datetime import datetime, timezone
import numpy as np
import pyiqfeed as iq
import redis
from zoneinfo import ZoneInfo
import threading # Import threading

# Local imports
from scripts.dtn_iq_client import get_iqfeed_quote_conn, get_iqfeed_history_conn, launch_iqfeed_service_if_needed
from config.config import settings

# --- Configuration ---

REDIS_URL = settings.REDIS_URL
redis_client = redis.Redis.from_url(REDIS_URL)

class LiveTickListener(iq.SilentQuoteListener):
    """
    A listener that processes live data from QuoteConn, backfills historical ticks,
    and publishes raw ticks to Redis for the application server to process.
    """

    def __init__(self, name="LiveTickListener"):
        super().__init__(name)
        self.redis_client = redis_client
        self.source_timezone = ZoneInfo("America/New_York")

    def backfill_intraday_data(self, symbol: str, hist_conn: iq.HistoryConn):
        """On startup, fetch today's raw ticks from IQFeed to populate the cache."""
        logger.info(f"Backfilling intraday raw ticks for {symbol}...")
        try:
            # Fetch raw ticks for the current day instead of 1-second bars.
            today_ticks = hist_conn.request_ticks_for_days(
                ticker=symbol, num_days=1, ascend=True
            )

            if today_ticks is not None and len(today_ticks) > 0:
                cache_key = f"intraday_ticks:{symbol}"
                self.redis_client.delete(cache_key)
                
                # Use a pipeline for efficient bulk insertion
                pipeline = self.redis_client.pipeline()
                
                for tick in today_ticks:
                    # Convert IQFeed's timestamp parts to a UTC Unix timestamp
                    naive_dt = iq.date_us_to_datetime(tick['date'], tick['time'])
                    aware_dt = naive_dt.replace(tzinfo=self.source_timezone)
                    utc_timestamp = aware_dt.timestamp()

                    tick_data = {
                        "timestamp": utc_timestamp,
                        "price": float(tick['last']),
                        "volume": int(tick['last_sz']),
                    }
                    pipeline.rpush(cache_key, json.dumps(tick_data))
                
                pipeline.expire(cache_key, 86400) # Expire after 24 hours
                pipeline.execute()
                
                logger.info(f"Successfully backfilled {len(today_ticks)} raw ticks for {symbol}.")
            else:
                logger.info(f"No intraday tick data found to backfill for {symbol}.")
        except iq.NoDataError:
            logger.warning(f"No intraday tick data available to backfill for {symbol}.")
        except Exception as e:
            logger.error(f"Error during intraday tick backfill for {symbol}: {e}", exc_info=True)
    
    def _publish_tick(self, symbol: str, price: float, volume: int):
        """
        Publishes a single raw tick to a Redis channel for live streaming and
        adds it to a capped list for backfilling recent activity.
        """
        utc_timestamp = datetime.now(timezone.utc).timestamp()
        
        tick_data = {
            # "symbol": symbol,
            "price": price,
            "volume": volume,
            "timestamp": utc_timestamp
        }
        
        # Publish to the 'live_ticks' channel for real-time subscribers
        channel = f"live_ticks:{symbol}"
        self.redis_client.publish(channel, json.dumps(tick_data))

        # Add the tick to a capped list for new clients to backfill recent data
        cache_key = f"intraday_ticks:{symbol}"
        pipeline = self.redis_client.pipeline()
        pipeline.rpush(cache_key, json.dumps(tick_data))
        pipeline.expire(cache_key, 86400)
        pipeline.execute()

    def process_summary(self, summary_data: np.ndarray) -> None:
        """Handles summary messages, treating them as ticks with zero volume."""
        try:
            for summary in summary_data:
                symbol = summary['Symbol'].decode('utf-8')
                price = float(summary['Most Recent Trade'])
                # Summary messages are not trades, so volume is 0.
                if price > 0:
                    self._publish_tick(symbol, price, 0)
        except Exception as e:
            logger.error(f"Error processing SUMMARY data: {e}. Data: {summary_data}", exc_info=True)

    def process_update(self, update_data: np.ndarray) -> None:
        """Handles trade update messages and publishes them as raw ticks."""
        try:
            for trade in update_data:
                price = float(trade['Most Recent Trade'])
                volume = int(trade['Most Recent Trade Size'])
                
                if price <= 0 or volume <= 0:
                    continue 

                symbol = trade['Symbol'].decode('utf-8')
                self._publish_tick(symbol, price, volume)
        except Exception as e:
            logger.error(f"Error processing TRADE data: {e}. Data: {update_data}", exc_info=True)

def update_watched_symbols(r_client, quote_conn, hist_conn, listener, watched_symbols_set):
    """
    Fetches the latest symbols from Redis and updates the watched symbols.
    """
    logger.info("Checking for symbol updates from Redis...")
    try:
        symbols_data_json = r_client.get("dtn:ingestion:symbols")
        if not symbols_data_json:
            logger.warning("No symbols found in Redis key 'dtn:ingestion:symbols'. Unwatching all current symbols.")
            symbols_from_redis = set()
        else:
            symbols_data = json.loads(symbols_data_json)
            symbols_from_redis = {item["symbol"] for item in symbols_data if "symbol" in item}

        symbols_to_add = symbols_from_redis - watched_symbols_set
        symbols_to_remove = watched_symbols_set - symbols_from_redis

        for symbol in symbols_to_add:
            listener.backfill_intraday_data(symbol, hist_conn)
            quote_conn.trades_watch(symbol)
            logger.info(f"Dynamically added and watching {symbol} for live tick updates.")
            watched_symbols_set.add(symbol)
        
        for symbol in symbols_to_remove:
            quote_conn.unwatch(symbol)
            logger.info(f"Dynamically unwatched {symbol}.")
            watched_symbols_set.remove(symbol)

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding symbols from Redis: {e}")
    except Exception as e:
        logger.error(f"Error updating watched symbols: {e}", exc_info=True)

def redis_pubsub_listener(r_client, quote_conn, hist_conn, listener, watched_symbols_set):
    """
    Listens for messages on the Redis Pub/Sub channel and triggers symbol updates.
    """
    pubsub = r_client.pubsub()
    pubsub.subscribe("dtn:ingestion:symbol_updates")
    logger.info("Subscribed to Redis channel 'dtn:ingestion:symbol_updates'.")

    for message in pubsub.listen():
        if message['type'] == 'message':
            logger.info(f"Received Redis Pub/Sub message: {message['data']}")
            update_watched_symbols(r_client, quote_conn, hist_conn, listener, watched_symbols_set)

def main():
    """
    Main function to start listening to live data.
    """
    launch_iqfeed_service_if_needed()
    
    try:
        REDIS_URL = settings.REDIS_URL
        r = redis.Redis.from_url(REDIS_URL)
        r.ping()
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}. Exiting.")
        return

    quote_conn = get_iqfeed_quote_conn()
    hist_conn = get_iqfeed_history_conn()

    if not quote_conn or not hist_conn:
        logger.error("Could not get IQFeed connections. Exiting.")
        return

    listener = LiveTickListener()
    quote_conn.add_listener(listener)
    
    watched_symbols = set() # Keep track of symbols currently being watched

    with iq.ConnConnector([quote_conn, hist_conn]):
        # Initial load of symbols
        update_watched_symbols(r, quote_conn, hist_conn, listener, watched_symbols)

        # Start Redis Pub/Sub listener in a separate thread
        pubsub_thread = threading.Thread(
            target=redis_pubsub_listener,
            args=(r, quote_conn, hist_conn, listener, watched_symbols),
            daemon=True # Daemon thread exits when the main program exits
        )
        pubsub_thread.start()

        try:
            logger.info("Ingestion service is running. Press Ctrl+C to stop.")
            # Keep the main thread alive
            while True:
                time.sleep(1) 
        except KeyboardInterrupt:
            logger.info("Stopping live data ingestion.")
            for symbol in watched_symbols:
                quote_conn.unwatch(symbol)

if __name__ == "__main__":
    main()