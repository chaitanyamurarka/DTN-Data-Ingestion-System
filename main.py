from fastapi import FastAPI, Query, HTTPException, Request
import redis
import json
import pandas as pd
from io import StringIO
import asyncio
from concurrent.futures import ProcessPoolExecutor
from pydantic import BaseModel
from contextlib import asynccontextmanager
from config.config import settings
from config.logging_config import logger # Import the logger

# --- New Lifespan Manager ---
# This replaces the deprecated on_event('startup') and on_event('shutdown')
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup
    logger.info("Application startup initiated.")
    global r, executor
    try:
        REDIS_URL = settings.REDIS_URL
        r = redis.Redis.from_url(REDIS_URL, decode_responses=False) # Keep decode_responses=False for raw bytes
        r.ping()
        logger.info("Successfully connected to Redis!")
    except redis.exceptions.ConnectionError as e:
        logger.critical(f"Could not connect to Redis: {e}. Please ensure Redis server is running and accessible.")
        # In a real app, you might not want to raise here, but handle it gracefully
        raise RuntimeError(f"Could not connect to Redis: {e}")
    
    executor = ProcessPoolExecutor()
    logger.info("ProcessPoolExecutor initialized.")
    
    yield # The application runs here

    # Code to run on shutdown
    executor.shutdown(wait=True)
    logger.info("ProcessPoolExecutor shut down.")
    logger.info("Application shutdown initiated.")

# Initialize FastAPI app with the new lifespan manager
app = FastAPI(lifespan=lifespan)

# --- Global variables that will be initialized in the lifespan ---
r: redis.Redis = None
executor: ProcessPoolExecutor = None


# Middleware to log incoming requests
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url} from client {request.client.host}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code} for {request.method} {request.url}")
    return response

class SymbolUpdate(BaseModel):
    symbol: str
    exchange: str

# This CPU-bound function is now correct as it expects a string
def search_dataframe(df_json: str, search_string: str):
    logger.debug(f"Searching DataFrame for string: '{search_string}'")
    # StringIO now correctly receives a string
    df = pd.read_json(StringIO(df_json))
    if search_string:
        search_string_lower = search_string.lower()
        df = df[
            df['symbol'].str.lower().str.contains(search_string_lower) |
            df['description'].str.lower().str.contains(search_string_lower)
        ]
    logger.debug(f"DataFrame search completed. Rows found: {len(df)}")
    return df.to_json(orient='records')


@app.post("/set_ingestion_symbols/")
async def set_ingestion_symbols(symbols: list[SymbolUpdate]):
    """
    Sets the complete list of symbols to be ingested by the OHLC and Live Tick services.
    This overwrites any existing list.
    """
    logger.info(f"Received request to set ingestion symbols. Payload: {len(symbols)} symbols.")
    try:
        symbols_data = [s.dict() for s in symbols]
        logger.debug(f"Prepared symbols data for Redis: {symbols_data}")
        r.set("dtn:ingestion:symbols", json.dumps(symbols_data))
        r.publish("dtn:ingestion:symbol_updates", "symbols_updated")
        logger.info("Ingestion symbols set successfully in Redis and update published.")
        return {"message": "Ingestion symbols set successfully"}
    except Exception as e:
        logger.error(f"Failed to set ingestion symbols: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to set ingestion symbols: {e}")

@app.post("/add_ingestion_symbol/")
async def add_ingestion_symbol(symbol_data: SymbolUpdate):
    """
    Adds a single symbol to the list of symbols to be ingested.
    """
    logger.info(f"Received request to add ingestion symbol: {symbol_data.symbol} (Exchange: {symbol_data.exchange})")
    try:
        current_symbols_json = r.get("dtn:ingestion:symbols")
        current_symbols = json.loads(current_symbols_json) if current_symbols_json else []
        
        new_symbol_dict = symbol_data.dict()
        
        if not any(s['symbol'] == new_symbol_dict['symbol'] and s['exchange'] == new_symbol_dict['exchange'] for s in current_symbols):
            current_symbols.append(new_symbol_dict)
            r.set("dtn:ingestion:symbols", json.dumps(current_symbols))
            r.publish("dtn:ingestion:symbol_updates", "symbols_updated")
            logger.info(f"Symbol {symbol_data.symbol} added successfully. Total symbols: {len(current_symbols)}")
            return {"message": f"Symbol {symbol_data.symbol} added successfully."}
        else:
            logger.info(f"Symbol {symbol_data.symbol} (Exchange: {symbol_data.exchange}) already exists. Skipping.")
            return {"message": f"Symbol {symbol_data.symbol} (Exchange: {symbol_data.exchange}) already exists.", "status": "skipped"}
    except Exception as e:
        logger.error(f"Failed to add ingestion symbol: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add ingestion symbol: {e}")


@app.get("/search_symbols/")
async def search_symbols(
    search_string: str = Query(None, description="Optional search string for symbol or description"),
    exchange: str = Query(None, description="Optional exchange to filter by (e.g., NYSE, CME)"),
    security_type: str = Query(None, description="Optional security type to filter by (e.g., STOCK, FUTURES)")
):
    logger.info(f"Received request to search symbols. Search string: '{search_string}', Exchange: '{exchange}', Security Type: '{security_type}'")
    all_keys = r.keys("symbols:*:*")
    logger.debug(f"Found {len(all_keys)} potential Redis keys for symbols.")
    
    filtered_keys = []
    for key in all_keys:
        decoded_key = key.decode('utf-8')
        parts = decoded_key.split(':')
        key_exchange, key_security_type = parts[1], parts[2]

        if (exchange is None or key_exchange.lower() == exchange.lower()) and \
           (security_type is None or key_security_type.lower() == security_type.lower()):
            filtered_keys.append(key)
    
    logger.debug(f"Filtered down to {len(filtered_keys)} Redis keys.")
    if not filtered_keys:
        return []

    # *** FIX IS HERE ***
    # Fetch values and decode them from bytes to strings immediately.
    all_dfs_json_strings = []
    for key in filtered_keys:
        value_bytes = r.get(key)
        if value_bytes:
            all_dfs_json_strings.append(value_bytes.decode('utf-8'))

    logger.debug(f"Fetched and decoded {len(all_dfs_json_strings)} DataFrames from Redis.")
    if not all_dfs_json_strings:
        return []

    loop = asyncio.get_running_loop()
    search_tasks = [
        loop.run_in_executor(executor, search_dataframe, df_json_str, search_string)
        for df_json_str in all_dfs_json_strings
    ]
    
    results_json = await asyncio.gather(*search_tasks)
    logger.debug(f"Completed {len(results_json)} DataFrame search tasks.")

    combined_df = pd.DataFrame()
    for res_json in results_json:
        if res_json:
            res_df = pd.read_json(StringIO(res_json))
            combined_df = pd.concat([combined_df, res_df], ignore_index=True)
    
    if not combined_df.empty:
        combined_df.drop_duplicates(subset=['symbol', 'exchange', 'securityType'], inplace=True)
    
    logger.info(f"Combined search results. Total unique symbols found: {len(combined_df)}")
    return combined_df.to_dict(orient='records')


if __name__ == "__main__":
    logger.info("Starting Uvicorn server...")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8500, log_level="info")
    logger.info("Uvicorn server stopped.")