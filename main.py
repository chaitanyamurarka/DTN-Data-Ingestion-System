from fastapi import FastAPI, Query, HTTPException, Request
import redis
import json
import pandas as pd
from io import StringIO
import asyncio
from concurrent.futures import ProcessPoolExecutor
from pydantic import BaseModel
from config import settings
from logging_config import logger # Import the logger

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    logger.info("Application startup initiated.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated.")

# Redis connection (similar to process_symbols.py)
try:
    REDIS_URL = settings.REDIS_URL
    r = redis.Redis.from_url(REDIS_URL)    
    r.ping()
    logger.info("Successfully connected to Redis!")
except redis.exceptions.ConnectionError as e:
    logger.critical(f"Could not connect to Redis: {e}. Please ensure Redis server is running and accessible.")
    raise HTTPException(status_code=500, detail="Could not connect to Redis")

# Middleware to log incoming requests
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url} from client {request.client.host}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code} for {request.method} {request.url}")
    return response

# Initialize ProcessPoolExecutor for CPU-bound tasks
executor = ProcessPoolExecutor()

class SymbolUpdate(BaseModel):
    symbol: str
    exchange: str

@app.post("/set_ingestion_symbols/")
async def set_ingestion_symbols(symbols: list[SymbolUpdate]):
    """
    Sets the complete list of symbols to be ingested by the OHLC and Live Tick services.
    The symbols are stored in Redis under the key 'dtn:ingestion:symbols'.
    This overwrites any existing list.
    """
    logger.info(f"Received request to set ingestion symbols. Payload: {len(symbols)} symbols.")
    try:
        # Convert the list of Pydantic models to a list of dictionaries
        symbols_data = [s.dict() for s in symbols]
        logger.debug(f"Prepared symbols data for Redis: {symbols_data}")
        r.set("dtn:ingestion:symbols", json.dumps(symbols_data))
        r.publish("dtn:ingestion:symbol_updates", "symbols_updated") # Publish update message
        logger.info("Ingestion symbols set successfully in Redis and update published.")
        return {"message": "Ingestion symbols set successfully"}
    except Exception as e:
        logger.error(f"Failed to set ingestion symbols: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to set ingestion symbols: {e}")

@app.post("/add_ingestion_symbol/")
async def add_ingestion_symbol(symbol_data: SymbolUpdate):
    """
    Adds a single symbol to the list of symbols to be ingested.
    If the symbol (with its exchange) already exists, it will not be added again.
    """
    logger.info(f"Received request to add ingestion symbol: {symbol_data.symbol} (Exchange: {symbol_data.exchange})")
    try:
        current_symbols_json = r.get("dtn:ingestion:symbols")
        if current_symbols_json:
            current_symbols = json.loads(current_symbols_json)
            logger.debug(f"Current symbols in Redis: {len(current_symbols)} symbols.")
        else:
            current_symbols = []
            logger.debug("No existing symbols found in Redis.")

        new_symbol_dict = symbol_data.dict()
        
        # Check for duplicates based on symbol and exchange
        if not any(s['symbol'] == new_symbol_dict['symbol'] and s['exchange'] == new_symbol_dict['exchange'] for s in current_symbols):
            current_symbols.append(new_symbol_dict)
            r.set("dtn:ingestion:symbols", json.dumps(current_symbols))
            r.publish("dtn:ingestion:symbol_updates", "symbols_updated") # Publish update message
            logger.info(f"Symbol {symbol_data.symbol} added successfully. Total symbols: {len(current_symbols)}")
            return {"message": f"Symbol {symbol_data.symbol} added successfully."}
        else:
            logger.info(f"Symbol {symbol_data.symbol} (Exchange: {symbol_data.exchange}) already exists in the ingestion list. Skipping addition.")
            return {"message": f"Symbol {symbol_data.symbol} (Exchange: {symbol_data.exchange}) already exists in the ingestion list.", "status": "skipped"}
    except Exception as e:
        logger.error(f"Failed to add ingestion symbol: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add ingestion symbol: {e}")

def search_dataframe(df_json, search_string):
    logger.debug(f"Searching DataFrame for string: '{search_string}'")
    df = pd.read_json(StringIO(df_json))
    if search_string:
        search_string_lower = search_string.lower()
        # Search in both 'symbol' and 'description' columns
        df = df[
            df['symbol'].str.lower().str.contains(search_string_lower) |
            df['description'].str.lower().str.contains(search_string_lower)
        ]
    logger.debug(f"DataFrame search completed. Rows found: {len(df)}")
    return df.to_json(orient='records')

@app.get("/search_symbols/")
async def search_symbols(
    search_string: str = Query(None, description="Optional search string for symbol or description"),
    exchange: str = Query(None, description="Optional exchange to filter by (e.g., NYSE, CME)"),
    security_type: str = Query(None, description="Optional security type to filter by (e.g., STOCK, FUTURES)")
):
    logger.info(f"Received request to search symbols. Search string: '{search_string}', Exchange: '{exchange}', Security Type: '{security_type}'")
    all_keys = r.keys("symbols:*:*") # Get all keys matching the pattern symbols:<exchange>:<securityType>
    logger.debug(f"Found {len(all_keys)} potential Redis keys for symbols.")
    
    # Filter keys based on provided exchange and security_type
    filtered_keys = []
    for key in all_keys:
        parts = key.split(':')
        key_exchange = parts[1]
        key_security_type = parts[2]

        if (exchange is None or key_exchange.lower() == exchange.lower()) and \
           (security_type is None or key_security_type.lower() == security_type.lower()):
            filtered_keys.append(key)
    logger.debug(f"Filtered down to {len(filtered_keys)} Redis keys.")

    if not filtered_keys:
        logger.info("No matching keys found for the given search criteria.")
        return [] # No matching keys found

    # Asynchronously fetch data from Redis for all filtered keys
    # Use a list comprehension to create a list of coroutines
    data_futures = [r.get(key) for key in filtered_keys]
    
    # Execute all Redis GET operations concurrently
    # Note: redis-py's get is synchronous, but we can run them in parallel using asyncio.gather
    # For true async Redis, an async Redis client would be needed (e.g., aioredis)
    # For now, we'll simulate concurrency by just getting them in a loop,
    # but the real parallelism will come from the ProcessPoolExecutor.
    
    # Since redis.get is synchronous, we'll fetch them sequentially for now.
    # If performance is critical here, an async redis client would be necessary.
    all_dfs_json = [r.get(key) for key in filtered_keys if r.get(key) is not None]
    logger.debug(f"Fetched {len(all_dfs_json)} DataFrames from Redis.")

    if not all_dfs_json:
        logger.info("No DataFrame content retrieved from Redis for filtered keys.")
        return []

    # Use ProcessPoolExecutor for parallel processing of DataFrames
    loop = asyncio.get_running_loop()
    search_tasks = [
        loop.run_in_executor(executor, search_dataframe, df_json, search_string)
        for df_json in all_dfs_json
    ]
    
    # Wait for all search tasks to complete
    results_json = await asyncio.gather(*search_tasks)
    logger.debug(f"Completed {len(results_json)} DataFrame search tasks.")

    # Combine results from all DataFrames
    combined_df = pd.DataFrame()
    for res_json in results_json:
        if res_json:
            combined_df = pd.concat([combined_df, pd.read_json(StringIO(res_json))])
    
    # Remove duplicates if any (e.g., if a symbol appears in multiple security types for the same exchange)
    combined_df.drop_duplicates(subset=['symbol', 'exchange', 'securityType'], inplace=True)
    logger.info(f"Combined search results. Total unique symbols found: {len(combined_df)}")

    return combined_df.to_dict(orient='records')

if __name__ == "__main__":
    logger.info("Starting Uvicorn server...")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    logger.info("Uvicorn server stopped.")

