from fastapi import FastAPI, Query, HTTPException
import redis
import json
import pandas as pd
from io import StringIO
import asyncio
from concurrent.futures import ProcessPoolExecutor
from pydantic import BaseModel

app = FastAPI()

# Redis connection (similar to process_symbols.py)
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.ping()
    print("Successfully connected to Redis!")
except redis.exceptions.ConnectionError as e:
    print(f"Could not connect to Redis: {e}")
    print("Please ensure Redis server is running and accessible.")
    # Exit is not suitable for a web server, raise an exception or handle gracefully
    raise HTTPException(status_code=500, detail="Could not connect to Redis")

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
    try:
        # Convert the list of Pydantic models to a list of dictionaries
        symbols_data = [s.dict() for s in symbols]
        r.set("dtn:ingestion:symbols", json.dumps(symbols_data))
        r.publish("dtn:ingestion:symbol_updates", "symbols_updated") # Publish update message
        return {"message": "Ingestion symbols set successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to set ingestion symbols: {e}")

@app.post("/add_ingestion_symbol/")
async def add_ingestion_symbol(symbol_data: SymbolUpdate):
    """
    Adds a single symbol to the list of symbols to be ingested.
    If the symbol (with its exchange) already exists, it will not be added again.
    """
    try:
        current_symbols_json = r.get("dtn:ingestion:symbols")
        if current_symbols_json:
            current_symbols = json.loads(current_symbols_json)
        else:
            current_symbols = []

        new_symbol_dict = symbol_data.dict()
        
        # Check for duplicates based on symbol and exchange
        if not any(s['symbol'] == new_symbol_dict['symbol'] and s['exchange'] == new_symbol_dict['exchange'] for s in current_symbols):
            current_symbols.append(new_symbol_dict)
            r.set("dtn:ingestion:symbols", json.dumps(current_symbols))
            r.publish("dtn:ingestion:symbol_updates", "symbols_updated") # Publish update message
            return {"message": f"Symbol {symbol_data.symbol} added successfully."}
        else:
            return {"message": f"Symbol {symbol_data.symbol} (Exchange: {symbol_data.exchange}) already exists in the ingestion list.", "status": "skipped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add ingestion symbol: {e}")

def search_dataframe(df_json, search_string):
    df = pd.read_json(StringIO(df_json))
    if search_string:
        search_string_lower = search_string.lower()
        # Search in both 'symbol' and 'description' columns
        df = df[
            df['symbol'].str.lower().str.contains(search_string_lower) |
            df['description'].str.lower().str.contains(search_string_lower)
        ]
    return df.to_json(orient='records')

@app.get("/search_symbols/")
async def search_symbols(
    search_string: str = Query(None, description="Optional search string for symbol or description"),
    exchange: str = Query(None, description="Optional exchange to filter by (e.g., NYSE, CME)"),
    security_type: str = Query(None, description="Optional security type to filter by (e.g., STOCK, FUTURES)")
):
    all_keys = r.keys("symbols:*:*") # Get all keys matching the pattern symbols:<exchange>:<securityType>
    
    # Filter keys based on provided exchange and security_type
    filtered_keys = []
    for key in all_keys:
        parts = key.split(':')
        key_exchange = parts[1]
        key_security_type = parts[2]

        if (exchange is None or key_exchange.lower() == exchange.lower()) and \
           (security_type is None or key_security_type.lower() == security_type.lower()):
            filtered_keys.append(key)

    if not filtered_keys:
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

    if not all_dfs_json:
        return []

    # Use ProcessPoolExecutor for parallel processing of DataFrames
    loop = asyncio.get_running_loop()
    search_tasks = [
        loop.run_in_executor(executor, search_dataframe, df_json, search_string)
        for df_json in all_dfs_json
    ]
    
    # Wait for all search tasks to complete
    results_json = await asyncio.gather(*search_tasks)

    # Combine results from all DataFrames
    combined_df = pd.DataFrame()
    for res_json in results_json:
        if res_json:
            combined_df = pd.concat([combined_df, pd.read_json(StringIO(res_json))])
    
    # Remove duplicates if any (e.g., if a symbol appears in multiple security types for the same exchange)
    combined_df.drop_duplicates(subset=['symbol', 'exchange', 'securityType'], inplace=True)

    return combined_df.to_dict(orient='records')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

