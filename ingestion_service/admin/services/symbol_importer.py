import os
import zipfile
import pandas as pd
import logging
from typing import List, Set,Dict
from ..models.schemas import SymbolCreate

logger = logging.getLogger(__name__)

class SymbolImporter:
    ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "CME", "EUREX"}
    
    def __init__(self, symbol_manager):
        self.symbol_manager = symbol_manager
    
    def import_from_dtn_zip(self, zip_path: str) -> Dict[str, int]:
        """Import symbols from DTN by_exchange.zip file"""
        imported_counts = {}
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                # List all files in the zip
                for file_info in zip_file.filelist:
                    # Parse exchange from path (e.g., "by_exchange/NYSE/STOCK.csv")
                    path_parts = file_info.filename.split('/')
                    if len(path_parts) >= 3 and path_parts[1] in self.ALLOWED_EXCHANGES:
                        exchange = path_parts[1]
                        security_type = path_parts[2].replace('.csv', '')
                        
                        # Read CSV file
                        with zip_file.open(file_info) as csv_file:
                            df = pd.read_csv(csv_file)
                            
                            # Import each symbol
                            count = 0
                            for _, row in df.iterrows():
                                try:
                                    symbol_data = SymbolCreate(
                                        symbol=row['symbol'],
                                        exchange=exchange,
                                        security_type=security_type,
                                        description=row.get('description', ''),
                                        historical_days=30,  # Default
                                        backfill_minutes=120,  # Default 2 hours
                                        added_by="dtn_import"
                                    )
                                    self.symbol_manager.add_symbol(symbol_data)
                                    count += 1
                                except Exception as e:
                                    logger.warning(f"Failed to import {row['symbol']}: {e}")
                            
                            key = f"{exchange}_{security_type}"
                            imported_counts[key] = count
                            logger.info(f"Imported {count} symbols for {key}")
            
            return imported_counts
            
        except Exception as e:
            logger.error(f"Error importing DTN symbols: {e}")
            raise