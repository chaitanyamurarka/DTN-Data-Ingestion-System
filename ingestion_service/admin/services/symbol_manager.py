import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import redis
import json

from config import settings
from models.schemas import Symbol, SymbolCreate, SymbolUpdate, SymbolFilter

logger = logging.getLogger(__name__)

class SymbolManager:
    def __init__(self):
        self.influx_client = InfluxDBClient(
            url=settings.INFLUX_URL,
            token=settings.INFLUX_TOKEN,
            org=settings.INFLUX_ORG
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.influx_client.query_api()
        self.redis_client = redis.from_url(settings.REDIS_URL)
        self.bucket = "symbol_management"  # Separate bucket for symbols
        
    def add_symbol(self, symbol: SymbolCreate) -> Symbol:
        """Add a new symbol to the system"""
        try:
            # Create measurement name
            measurement = f"symbol_{symbol.exchange}_{symbol.security_type}"
            
            # Create InfluxDB point
            point = Point(measurement) \
                .tag("symbol", symbol.symbol) \
                .tag("exchange", symbol.exchange) \
                .tag("security_type", symbol.security_type) \
                .field("description", symbol.description) \
                .field("active", True) \
                .field("historical_days", symbol.historical_days) \
                .field("backfill_minutes", symbol.backfill_minutes) \
                .field("added_by", symbol.added_by) \
                .time(datetime.utcnow(), WritePrecision.NS)
            
            self.write_api.write(bucket=self.bucket, record=point)
            
            # Cache in Redis for quick access
            cache_key = f"symbol:{symbol.symbol}"
            self.redis_client.setex(
                cache_key,
                86400,  # 24 hour TTL
                json.dumps(symbol.dict())
            )
            
            logger.info(f"Added symbol {symbol.symbol} to {measurement}")
            return Symbol(**symbol.dict(), id=symbol.symbol, created_at=datetime.utcnow())
            
        except Exception as e:
            logger.error(f"Error adding symbol {symbol.symbol}: {e}")
            raise
    
    def search_symbols(self, filter: SymbolFilter) -> List[Symbol]:
        """Search symbols with filtering"""
        # Build Flux query based on filters
        query_parts = []
        
        if filter.exchanges:
            exchanges_str = "|".join(filter.exchanges)
            query_parts.append(f'r.exchange =~ /({exchanges_str})/')
        
        if filter.security_types:
            types_str = "|".join(filter.security_types)
            query_parts.append(f'r.security_type =~ /({types_str})/')
        
        if filter.search_text:
            # Search in both symbol and description
            query_parts.append(
                f'(r.symbol =~ /{filter.search_text}/i or ' +
                f'r._value =~ /{filter.search_text}/i)'
            )
        
        if filter.active is not None:
            query_parts.append(f'r.active == {str(filter.active).lower()}')
        
        where_clause = " and ".join(query_parts) if query_parts else "true"
        
        flux_query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r._measurement =~ /^symbol_/)
          |> filter(fn: (r) => r._field == "description")
          |> filter(fn: (r) => {where_clause})
          |> last()
          |> group()
        '''
        
        try:
            tables = self.query_api.query(query=flux_query)
            symbols = []
            
            for table in tables:
                for record in table.records:
                    symbol_data = {
                        "id": record["symbol"],
                        "symbol": record["symbol"],
                        "exchange": record["exchange"],
                        "security_type": record["security_type"],
                        "description": record.get_value(),
                        "active": True,  # Will need separate query for other fields
                        "created_at": record.get_time()
                    }
                    symbols.append(Symbol(**symbol_data))
            
            return symbols
            
        except Exception as e:
            logger.error(f"Error searching symbols: {e}")
            return []
    
    def update_symbol(self, symbol_id: str, update: SymbolUpdate) -> Optional[Symbol]:
        """Update symbol configuration"""
        # Implementation for updating symbol settings
        pass
    
    def delete_symbol(self, symbol_id: str) -> bool:
        """Mark symbol as inactive (soft delete)"""
        # Implementation for soft deleting symbols
        pass
    
    def get_symbol_stats(self, symbol_id: str) -> Dict[str, Any]:
        """Get data availability statistics for a symbol"""
        # Query InfluxDB for available data ranges
        pass