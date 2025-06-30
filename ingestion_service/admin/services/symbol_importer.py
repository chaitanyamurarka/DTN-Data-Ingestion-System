import os
import zipfile
import pandas as pd
import logging
from typing import List, Set,Dict
from models.schemas import SymbolCreate, SecurityTypeEnum 
logger = logging.getLogger(__name__)

class SymbolImporter:
    ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "CME", "EUREX"}
    
    SECURITY_TYPE_MAP = {
        "EQUITY": SecurityTypeEnum.STOCK,
        "FUTURE": SecurityTypeEnum.FUTURE,
        "FOPTION": SecurityTypeEnum.OPTION,
        "IEOPTION": SecurityTypeEnum.OPTION,
        "INDEX": SecurityTypeEnum.INDEX,
        "FOREX": SecurityTypeEnum.FOREX,
        # Add other mappings as needed based on filenames in the zip
        "BONDS": "BOND", # Example if you add BOND to your Enum
        "MUTUAL": "MUTUAL_FUND" # Example
    }

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

    def search_in_dtn_zip(self, symbol_query: str) -> List[Dict]:
        """Search for a symbol within the uploaded DTN zip file."""
        zip_path = "uploads/by_exchange.zip"  # A known location for the file
        if not os.path.exists(zip_path):
            raise FileNotFoundError("DTN symbols file not found. Please upload it via the Import page.")

        matches = []
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                for file_info in zip_file.infolist():
                    path_parts = file_info.filename.split('/')

                    # **** CORRECTED LOGIC HERE ****
                    # The exchange is at index 2, and we need at least 4 parts in the path
                    if len(path_parts) >= 4 and path_parts[2] in self.ALLOWED_EXCHANGES and file_info.filename.endswith('.csv'):
                        exchange = path_parts[2]
                        raw_security_type = path_parts[3].replace('.csv', '')
                        # Use the map to get the correct enum value, skip if not in map
                        security_type = self.SECURITY_TYPE_MAP.get(raw_security_type.upper())
                        
                        if not security_type:
                            continue

                        with zip_file.open(file_info) as csv_file:
                            try:
                                df = pd.read_csv(csv_file)
                                # Ensure the 'symbol' column exists
                                if 'symbol' in df.columns:
                                    # Case-insensitive search for the symbol
                                    result_df = df[df['symbol'].str.contains(symbol_query, case=False, na=False)]

                                    for _, row in result_df.iterrows():
                                        matches.append({
                                            "symbol": row['symbol'],
                                            "exchange": exchange,
                                            "security_type": security_type, # This will now be the correct value (e.g., "STOCK")
                                            "description": row.get('description', '')
                                        })
                            except Exception as read_error:
                                logger.warning(f"Could not read or process {file_info.filename}: {read_error}")
                                
        except Exception as e:
            logger.error(f"Error searching DTN symbols zip: {e}")
            raise

        return matches