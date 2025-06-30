from typing import Dict, List, Optional,Any
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
import re
import logging

from config import settings

logger = logging.getLogger(__name__)

class DataMonitor:
    def __init__(self):
        self.influx_client = InfluxDBClient(
            url=settings.INFLUX_URL,
            token=settings.INFLUX_TOKEN,
            org=settings.INFLUX_ORG
        )
        self.query_api = self.influx_client.query_api()
        
    def get_symbol_data_availability(self, symbol: str) -> Dict[str, Dict]:
        """Get data availability for all timeframes of a symbol"""
        availability = {}
        
        # Define timeframes to check
        timeframes = [
            "1s", "5s", "10s", "15s", "30s", "45s",
            "1m", "5m", "10m", "15m", "30m", "45m",
            "1h", "1d"
        ]
        
        for tf in timeframes:
            # Query for first and last data points
            measurement_regex = f"^ohlc_{re.escape(symbol)}_\\d{{8}}_{tf}$"
            
            flux_query = f'''
            first_time = from(bucket: "{settings.INFLUX_BUCKET}")
              |> range(start: 0)
              |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{symbol}")
              |> first()
              |> keep(columns: ["_time"])
              
            last_time = from(bucket: "{settings.INFLUX_BUCKET}")
              |> range(start: 0)
              |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{symbol}")
              |> last()
              |> keep(columns: ["_time"])
            '''
            
            try:
                tables = self.query_api.query(flux_query)
                
                first_time = None
                last_time = None
                
                for table in tables:
                    if table.records:
                        if "first_time" in str(table):
                            first_time = table.records[0].get_time()
                        elif "last_time" in str(table):
                            last_time = table.records[0].get_time()
                
                if first_time and last_time:
                    availability[tf] = {
                        "first_timestamp": first_time.isoformat(),
                        "last_timestamp": last_time.isoformat(),
                        "duration_days": (last_time - first_time).days,
                        "data_points": self._estimate_data_points(first_time, last_time, tf)
                    }
                else:
                    availability[tf] = {
                        "first_timestamp": None,
                        "last_timestamp": None,
                        "duration_days": 0,
                        "data_points": 0
                    }
                    
            except Exception as e:
                logger.error(f"Error checking availability for {symbol} {tf}: {e}")
                availability[tf] = {
                    "error": str(e)
                }
        
        return availability
    
    def get_ingestion_statistics(self) -> Dict[str, Any]:
        """Get overall ingestion statistics"""
        stats = {
            "total_symbols": 0,
            "active_symbols": 0,
            "total_data_points": 0,
            "disk_usage_mb": 0,
            "last_update": None
        }
        
        # Query for symbol count
        flux_query = f'''
        from(bucket: "symbol_management")
          |> range(start: -30d)
          |> filter(fn: (r) => r._measurement =~ /^symbol_/)
          |> filter(fn: (r) => r._field == "active")
          |> last()
          |> group()
          |> count()
        '''
        
        try:
            tables = self.query_api.query(flux_query)
            if tables and tables[0].records:
                stats["total_symbols"] = len(tables[0].records)
                stats["active_symbols"] = sum(1 for r in tables[0].records if r.get_value())
        except Exception as e:
            logger.error(f"Error getting ingestion stats: {e}")
        
        return stats
    
    def _estimate_data_points(self, start: datetime, end: datetime, timeframe: str) -> int:
        """Estimate number of data points based on timeframe"""
        duration = end - start
        
        # Parse timeframe to seconds
        if timeframe.endswith('s'):
            seconds = int(timeframe[:-1])
        elif timeframe.endswith('m'):
            seconds = int(timeframe[:-1]) * 60
        elif timeframe.endswith('h'):
            seconds = int(timeframe[:-1]) * 3600
        elif timeframe.endswith('d'):
            seconds = int(timeframe[:-1]) * 86400
        else:
            return 0
        
        # Estimate based on market hours (6.5 hours per day, 5 days per week)
        market_hours_per_day = 6.5
        market_days_per_week = 5
        
        total_seconds = duration.total_seconds()
        market_seconds = total_seconds * (market_hours_per_day / 24) * (market_days_per_week / 7)
        
        return int(market_seconds / seconds)