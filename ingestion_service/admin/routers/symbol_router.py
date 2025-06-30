from fastapi import APIRouter, HTTPException, Query, Depends, File, UploadFile
from typing import List, Optional
from services.symbol_manager import SymbolManager
from models.schemas import Symbol, SymbolCreate, SymbolUpdate, SymbolFilter
import shutil
import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

def get_symbol_manager():
    return SymbolManager()

@router.post("/", response_model=Symbol)
async def create_symbol(
    symbol: SymbolCreate,
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Add a new symbol to the system"""
    try:
        return manager.add_symbol(symbol)
    except Exception as e:
        logger.error(f"Error creating symbol: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/search", response_model=List[Symbol])
async def search_symbols(
    exchanges: Optional[List[str]] = Query(None),
    security_types: Optional[List[str]] = Query(None),
    search_text: Optional[str] = Query(None),
    active: Optional[bool] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Search symbols with filters"""
    try:
        # Clean up empty query parameters
        if exchanges and exchanges == ['']:
            exchanges = None
        if security_types and security_types == ['']:
            security_types = None
        if search_text and search_text.strip() == '':
            search_text = None

        filter = SymbolFilter(
            exchanges=exchanges,
            security_types=security_types,
            search_text=search_text,
            active=active,
            limit=limit,
            offset=offset
        )
        
        logger.info(f"Searching symbols with filter: {filter}")
        symbols = manager.search_symbols(filter)
        logger.info(f"Found {len(symbols)} symbols")
        
        return symbols
    except Exception as e:
        logger.error(f"Error searching symbols: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching symbols: {str(e)}")

@router.get("/exchanges", response_model=List[str])
async def get_exchanges(
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Get list of available exchanges"""
    try:
        return manager.get_exchanges()
    except Exception as e:
        logger.error(f"Error getting exchanges: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/security-types", response_model=List[str])
async def get_security_types(
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Get list of available security types"""
    try:
        return manager.get_security_types()
    except Exception as e:
        logger.error(f"Error getting security types: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{symbol_id}", response_model=Symbol)
async def get_symbol(
    symbol_id: str,
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Get a specific symbol"""
    symbol = manager.get_symbol(symbol_id)
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return symbol

@router.patch("/{symbol_id}", response_model=Symbol)
async def update_symbol(
    symbol_id: str,
    update: SymbolUpdate,
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Update symbol configuration"""
    symbol = manager.update_symbol(symbol_id, update)
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return symbol

@router.delete("/{symbol_id}")
async def delete_symbol(
    symbol_id: str,
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Delete (deactivate) a symbol"""
    if not manager.delete_symbol(symbol_id):
        raise HTTPException(status_code=404, detail="Symbol not found")
    return {"status": "success", "message": f"Symbol {symbol_id} deactivated"}

@router.get("/{symbol_id}/stats")
async def get_symbol_stats(
    symbol_id: str,
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Get data availability statistics for a symbol"""
    try:
        stats = manager.get_symbol_stats(symbol_id)
        if not stats:
            raise HTTPException(status_code=404, detail="Symbol not found")
        return stats
    except Exception as e:
        logger.error(f"Error getting stats for {symbol_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk", response_model=dict)
async def bulk_create_symbols(
    symbols: List[SymbolCreate],
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Add multiple symbols in batch"""
    try:
        if not symbols:
            raise HTTPException(status_code=400, detail="No symbols provided")
        
        if len(symbols) > 1000:
            raise HTTPException(status_code=400, detail="Too many symbols (max 1000)")
        
        results = manager.bulk_add_symbols(symbols)
        return results
    except Exception as e:
        logger.error(f"Error in bulk symbol creation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lookup/{symbol_query}")
async def lookup_dtn_symbol(
    symbol_query: str,
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Looks up a symbol from the uploaded DTN zip file."""
    from services.symbol_importer import SymbolImporter
    
    try:
        importer = SymbolImporter(manager)
        results = importer.search_in_dtn_zip(symbol_query)
        return results
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error in symbol lookup: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred during lookup: {str(e)}")

@router.post("/import/dtn")
async def upload_dtn_symbols_file(
    file: UploadFile = File(...),
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Upload DTN symbols file and optionally import symbols"""
    try:
        # Validate file
        if not file.filename.endswith('.zip'):
            raise HTTPException(status_code=400, detail="File must be a ZIP file")
        
        # Create upload directory
        upload_dir = "uploads"
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, "by_exchange.zip")
        
        # Save file
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        logger.info(f"Successfully uploaded DTN symbols file: {file.filename}")
        
        return {
            "status": "success", 
            "message": "DTN symbols file uploaded successfully. You can now use the lookup feature.",
            "filename": file.filename,
            "size": os.path.getsize(file_path)
        }
        
    except Exception as e:
        logger.error(f"Error uploading DTN file: {e}")
        raise HTTPException(status_code=500, detail=f"Error uploading file: {e}")

@router.post("/import/dtn/process")
async def process_dtn_import(
    auto_add: bool = Query(False, description="Automatically add all symbols from DTN file"),
    exchanges: Optional[List[str]] = Query(None, description="Limit to specific exchanges"),
    security_types: Optional[List[str]] = Query(None, description="Limit to specific security types"),
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Process DTN file and import symbols"""
    from services.symbol_importer import SymbolImporter
    
    try:
        importer = SymbolImporter(manager)
        
        if not auto_add:
            # Just return count of available symbols
            stats = importer.get_import_stats()
            return {
                "status": "preview",
                "message": "DTN file analysis complete",
                "stats": stats
            }
        
        # Actually import symbols
        results = importer.import_from_dtn_zip(
            "uploads/by_exchange.zip",
            exchanges=exchanges,
            security_types=security_types
        )
        
        return {
            "status": "success",
            "message": "DTN symbols imported successfully",
            "imported": results
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="DTN file not found. Please upload it first.")
    except Exception as e:
        logger.error(f"Error processing DTN import: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing import: {e}")