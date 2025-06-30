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
            # Create measurement name - use string values, not enum objects
            exchange_str = symbol.exchange.value if hasattr(symbol.exchange, 'value') else str(symbol.exchange)
            security_type_str = symbol.security_type.value if hasattr(symbol.security_type, 'value') else str(symbol.security_type)
            
            measurement = f"symbol_{exchange_str}_{security_type_str}"
            
            # Create InfluxDB point - store enum values as strings
            point = Point(measurement) \
                .tag("symbol", symbol.symbol) \
                .tag("exchange", exchange_str) \
                .tag("security_type", security_type_str) \
                .field("description", symbol.description) \
                .field("active", True) \
                .field("historical_days", symbol.historical_days) \
                .field("backfill_minutes", symbol.backfill_minutes) \
                .field("added_by", symbol.added_by) \
                .time(datetime.utcnow(), WritePrecision.NS)
            
            self.write_api.write(bucket=self.bucket, record=point)
            
            # Cache in Redis for quick access
            cache_key = f"symbol:{symbol.symbol}"
            symbol_dict = symbol.dict()
            symbol_dict['exchange'] = exchange_str
            symbol_dict['security_type'] = security_type_str
            self.redis_client.setex(
                cache_key,
                86400,  # 24 hour TTL
                json.dumps(symbol_dict)
            )
            
            logger.info(f"Added symbol {symbol.symbol} to {measurement}")
            return Symbol(**symbol_dict, id=symbol.symbol, created_at=datetime.utcnow())
            
        except Exception as e:
            logger.error(f"Error adding symbol {symbol.symbol}: {e}")
            raise
    
    def search_symbols(self, filter: SymbolFilter) -> List[Symbol]:
        """Search symbols with filtering"""
        try:
            # Build the base query
            flux_query = f'''
                from(bucket: "{self.bucket}")
                  |> range(start: -30d)
                  |> filter(fn: (r) => r._measurement =~ /^symbol_/)
            '''
            
            # Add field filter for active symbols
            if filter.active is not None:
                flux_query += f'\n  |> filter(fn: (r) => r._field == "active" and r._value == {str(filter.active).lower()})'
            else:
                flux_query += '\n  |> filter(fn: (r) => r._field == "active")'
                
            # Add exchange filter
            if filter.exchanges:
                exchanges_pattern = "|".join(filter.exchanges)
                flux_query += f'\n  |> filter(fn: (r) => r.exchange =~ /^({exchanges_pattern})$/)'
            
            # Add security type filter
            if filter.security_types:
                types_pattern = "|".join(filter.security_types)
                flux_query += f'\n  |> filter(fn: (r) => r.security_type =~ /^({types_pattern})$/)'
                
            # Add symbol search filter
            if filter.search_text:
                flux_query += f'\n  |> filter(fn: (r) => r.symbol =~ /{filter.search_text}/i)'
            
            # Get the latest values and apply pagination
            flux_query += f'''
              |> last()
              |> limit(n: {filter.limit}, offset: {filter.offset})
            '''
            
            logger.info(f"Executing flux query: {flux_query}")
            tables = self.query_api.query(query=flux_query)
            
            symbols = []
            symbol_details = {}
            
            # First pass: collect all unique symbols
            for table in tables:
                for record in table.records:
                    symbol_name = record["symbol"]
                    if symbol_name not in symbol_details:
                        symbol_details[symbol_name] = {
                            "symbol": symbol_name,
                            "exchange": record["exchange"],
                            "security_type": record["security_type"],
                            "active": record.get_value() if record.get_field() == "active" else True,
                            "created_at": record.get_time(),
                            "description": "",
                            "historical_days": 30,
                            "backfill_minutes": 120
                        }
            
            # Second pass: get additional details for each symbol
            for symbol_name in symbol_details.keys():
                try:
                    details = self._get_symbol_details(symbol_name)
                    symbol_details[symbol_name].update(details)
                except Exception as e:
                    logger.warning(f"Could not get details for {symbol_name}: {e}")
            
            # Convert to Symbol objects
            for symbol_data in symbol_details.values():
                try:
                    symbol = Symbol(
                        id=symbol_data["symbol"],
                        **symbol_data
                    )
                    symbols.append(symbol)
                except Exception as e:
                    logger.warning(f"Could not create Symbol object for {symbol_data.get('symbol', 'unknown')}: {e}")
            
            logger.info(f"Found {len(symbols)} symbols matching criteria")
            return symbols
            
        except Exception as e:
            logger.error(f"Error searching symbols: {e}", exc_info=True)
            return []
    
    def _get_symbol_details(self, symbol: str) -> Dict[str, Any]:
        """Get additional details for a specific symbol"""
        details = {}
        
        # Query for all fields for this symbol
        flux_query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: -30d)
              |> filter(fn: (r) => r._measurement =~ /^symbol_/)
              |> filter(fn: (r) => r.symbol == "{symbol}")
              |> last()
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        
        try:
            tables = self.query_api.query(query=flux_query)
            if tables and tables[0].records:
                record = tables[0].records[0]
                
                # Extract available fields
                if hasattr(record, 'values') and record.values:
                    for key, value in record.values.items():
                        if key.startswith('_') or key in ['result', 'table']:
                            continue
                        if key == "description":
                            details["description"] = value or ""
                        elif key == "historical_days":
                            details["historical_days"] = int(value) if value else 30
                        elif key == "backfill_minutes":
                            details["backfill_minutes"] = int(value) if value else 120
                        elif key == "last_ingestion":
                            details["last_ingestion"] = value
                        elif key == "updated_at":
                            details["updated_at"] = value
                            
        except Exception as e:
            logger.warning(f"Error getting details for symbol {symbol}: {e}")
        
        return details
    
    def get_symbol(self, symbol_id: str) -> Optional[Symbol]:
        """Get a specific symbol by ID"""
        try:
            # Try Redis cache first
            cache_key = f"symbol:{symbol_id}"
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                symbol_data = json.loads(cached_data)
                # Ensure enum values are clean
                symbol_data['exchange'] = self._extract_enum_value(symbol_data.get('exchange', ''))
                symbol_data['security_type'] = self._extract_enum_value(symbol_data.get('security_type', ''))
                return Symbol(**symbol_data, id=symbol_id)
            
            # Query InfluxDB
            flux_query = f'''
                from(bucket: "{self.bucket}")
                  |> range(start: -30d)
                  |> filter(fn: (r) => r._measurement =~ /^symbol_/)
                  |> filter(fn: (r) => r.symbol == "{symbol_id}")
                  |> last()
                  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''
            
            tables = self.query_api.query(query=flux_query)
            if tables and tables[0].records:
                record = tables[0].records[0]
                
                # Extract clean enum values
                exchange = self._extract_enum_value(record["exchange"])
                security_type = self._extract_enum_value(record["security_type"])
                
                symbol_data = {
                    "id": symbol_id,
                    "symbol": symbol_id,
                    "exchange": exchange,
                    "security_type": security_type,
                    "active": record.values.get("active", True),
                    "description": record.values.get("description", ""),
                    "historical_days": int(record.values.get("historical_days", 30)),
                    "backfill_minutes": int(record.values.get("backfill_minutes", 120)),
                    "created_at": record.get_time(),
                    "updated_at": record.values.get("updated_at"),
                    "last_ingestion": record.values.get("last_ingestion")
                }
                
                return Symbol(**symbol_data)
                
        except Exception as e:
            logger.error(f"Error getting symbol {symbol_id}: {e}")
        
        return None
    
    def update_symbol(self, symbol_id: str, update: SymbolUpdate) -> Optional[Symbol]:
        """Update symbol configuration"""
        try:
            # Get existing symbol
            existing_symbol = self.get_symbol(symbol_id)
            if not existing_symbol:
                return None
            
            # Extract enum values as strings
            exchange_str = self._extract_enum_value(existing_symbol.exchange)
            security_type_str = self._extract_enum_value(existing_symbol.security_type)
            
            # Find the measurement for this symbol
            measurement = f"symbol_{exchange_str}_{security_type_str}"
            
            # Create update point with only changed fields
            point = Point(measurement) \
                .tag("symbol", symbol_id) \
                .tag("exchange", exchange_str) \
                .tag("security_type", security_type_str)
            
            # Add updated fields
            if update.description is not None:
                point = point.field("description", update.description)
            if update.historical_days is not None:
                point = point.field("historical_days", update.historical_days)
            if update.backfill_minutes is not None:
                point = point.field("backfill_minutes", update.backfill_minutes)
            if update.active is not None:
                point = point.field("active", update.active)
            
            # Add update timestamp
            point = point.field("updated_at", datetime.utcnow().isoformat())
            point = point.time(datetime.utcnow(), WritePrecision.NS)
            
            self.write_api.write(bucket=self.bucket, record=point)
            
            # Clear cache
            cache_key = f"symbol:{symbol_id}"
            self.redis_client.delete(cache_key)
            
            logger.info(f"Updated symbol {symbol_id}")
            return self.get_symbol(symbol_id)
            
        except Exception as e:
            logger.error(f"Error updating symbol {symbol_id}: {e}")
            return None
    
    def delete_symbol(self, symbol_id: str) -> bool:
        """Mark symbol as inactive (soft delete)"""
        try:
            existing_symbol = self.get_symbol(symbol_id)
            if not existing_symbol:
                return False
            
            # Extract enum values as strings
            exchange_str = self._extract_enum_value(existing_symbol.exchange)
            security_type_str = self._extract_enum_value(existing_symbol.security_type)
            
            measurement = f"symbol_{exchange_str}_{security_type_str}"
            
            # Mark as inactive
            point = Point(measurement) \
                .tag("symbol", symbol_id) \
                .tag("exchange", exchange_str) \
                .tag("security_type", security_type_str) \
                .field("active", False) \
                .field("updated_at", datetime.utcnow().isoformat()) \
                .time(datetime.utcnow(), WritePrecision.NS)
            
            self.write_api.write(bucket=self.bucket, record=point)
            
            # Clear cache
            cache_key = f"symbol:{symbol_id}"
            self.redis_client.delete(cache_key)
            
            logger.info(f"Deactivated symbol {symbol_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deactivating symbol {symbol_id}: {e}")
            return False
    
    def get_symbol_stats(self, symbol_id: str) -> Dict[str, Any]:
        """Get data availability statistics for a symbol"""
        try:
            stats = {
                "symbol": symbol_id,
                "total_data_points": 0,
                "available_timeframes": [],
                "date_range": {},
                "last_update": None
            }
            
            # Query data availability from main data bucket
            timeframes = ["1s", "5s", "10s", "15s", "30s", "45s", 
                         "1m", "5m", "10m", "15m", "30m", "45m", "1h", "1d"]
            
            for tf in timeframes:
                try:
                    # Check if data exists for this timeframe
                    flux_query = f'''
                        from(bucket: "{settings.INFLUX_BUCKET}")
                          |> range(start: -30d)
                          |> filter(fn: (r) => r._measurement =~ /ohlc_{symbol_id}_.*_{tf}$/)
                          |> filter(fn: (r) => r.symbol == "{symbol_id}")
                          |> count()
                    '''
                    
                    tables = self.query_api.query(query=flux_query)
                    if tables and tables[0].records:
                        count = tables[0].records[0].get_value()
                        if count > 0:
                            stats["available_timeframes"].append(tf)
                            stats["total_data_points"] += count
                            
                except Exception as e:
                    logger.warning(f"Error checking {tf} data for {symbol_id}: {e}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats for symbol {symbol_id}: {e}")
            return {}
    
    def bulk_add_symbols(self, symbols: List[SymbolCreate]) -> Dict[str, Any]:
        """Add multiple symbols in batch"""
        results = {
            "success": [],
            "failed": [],
            "total": len(symbols)
        }
        
        points = []
        
        for symbol in symbols:
            try:
                # Extract enum values as strings
                exchange_str = symbol.exchange.value if hasattr(symbol.exchange, 'value') else str(symbol.exchange)
                security_type_str = symbol.security_type.value if hasattr(symbol.security_type, 'value') else str(symbol.security_type)
                
                # Clean enum values if they contain the enum class name
                exchange_str = self._extract_enum_value(exchange_str)
                security_type_str = self._extract_enum_value(security_type_str)
                
                measurement = f"symbol_{exchange_str}_{security_type_str}"
                
                point = Point(measurement) \
                    .tag("symbol", symbol.symbol) \
                    .tag("exchange", exchange_str) \
                    .tag("security_type", security_type_str) \
                    .field("description", symbol.description) \
                    .field("active", True) \
                    .field("historical_days", symbol.historical_days) \
                    .field("backfill_minutes", symbol.backfill_minutes) \
                    .field("added_by", symbol.added_by) \
                    .time(datetime.utcnow(), WritePrecision.NS)
                
                points.append(point)
                results["success"].append(symbol.symbol)
                
            except Exception as e:
                logger.error(f"Error preparing symbol {symbol.symbol}: {e}")
                results["failed"].append({
                    "symbol": symbol.symbol,
                    "error": str(e)
                })
        
        # Batch write
        if points:
            try:
                self.write_api.write(bucket=self.bucket, record=points)
                logger.info(f"Successfully added {len(points)} symbols in batch")
            except Exception as e:
                logger.error(f"Error in batch write: {e}")
                # Move all to failed
                results["failed"].extend([{"symbol": s, "error": str(e)} for s in results["success"]])
                results["success"] = []
        
        return results
    
    def get_exchanges(self) -> List[str]:
        """Get list of all exchanges"""
        try:
            flux_query = f'''
                from(bucket: "{self.bucket}")
                  |> range(start: -30d)
                  |> filter(fn: (r) => r._measurement =~ /^symbol_/)
                  |> filter(fn: (r) => r._field == "active")
                  |> filter(fn: (r) => r._value == true)
                  |> group(columns: ["exchange"])
                  |> distinct(column: "exchange")
            '''
            
            tables = self.query_api.query(query=flux_query)
            exchanges = []
            
            for table in tables:
                for record in table.records:
                    exchange = record["exchange"]
                    if exchange not in exchanges:
                        exchanges.append(exchange)
            
            return sorted(exchanges)
            
        except Exception as e:
            logger.error(f"Error getting exchanges: {e}")
            return []
    
    def get_security_types(self) -> List[str]:
        """Get list of all security types"""
        try:
            flux_query = f'''
                from(bucket: "{self.bucket}")
                  |> range(start: -30d)
                  |> filter(fn: (r) => r._measurement =~ /^symbol_/)
                  |> filter(fn: (r) => r._field == "active")
                  |> filter(fn: (r) => r._value == true)
                  |> group(columns: ["security_type"])
                  |> distinct(column: "security_type")
            '''
            
            tables = self.query_api.query(query=flux_query)
            types = []
            
            for table in tables:
                for record in table.records:
                    sec_type = record["security_type"]
                    if sec_type not in types:
                        types.append(sec_type)
            
            return sorted(types)
            
        except Exception as e:
            logger.error(f"Error getting security types: {e}")
            return []