from fastapi import APIRouter, HTTPException, Query, Depends, File, UploadFile
from typing import List, Optional
from services.symbol_manager import SymbolManager
from models.schemas import Symbol, SymbolCreate, SymbolUpdate, SymbolFilter
import shutil
import os

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
    # Fix for empty query parameters being passed as ['']
    if exchanges and exchanges == ['']:
        exchanges = None
    if security_types and security_types == ['']:
        security_types = None

    filter = SymbolFilter(
        exchanges=exchanges,
        security_types=security_types,
        search_text=search_text,
        active=active,
        limit=limit,
        offset=offset
    )
    return manager.search_symbols(filter)

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

@router.get("/lookup/{symbol_query}")
async def lookup_dtn_symbol(
    symbol_query: str,
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Looks up a symbol from the uploaded DTN zip file."""
    from services.symbol_importer import SymbolImporter
    # We pass the symbol manager to the importer, although it's not used in the search function
    importer = SymbolImporter(manager)
    try:
        results = importer.search_in_dtn_zip(symbol_query)
        return results
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during lookup: {str(e)}")

# Also, modify the existing import endpoint to just save the file
@router.post("/import/dtn")
async def upload_dtn_symbols_file(file: UploadFile = File(...)):
    """Saves the uploaded DTN zip file to a known location for later lookups."""
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, "by_exchange.zip")

    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"status": "success", "message": "DTN symbols file uploaded successfully. You can now use the lookup feature."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {e}")
