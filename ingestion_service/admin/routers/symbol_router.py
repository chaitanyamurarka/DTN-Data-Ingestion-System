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

@router.post("/import/dtn")
async def import_dtn_symbols(
    file: UploadFile = File(...), # This is the key change to accept a file
    manager: SymbolManager = Depends(get_symbol_manager)
):
    """Import symbols from an uploaded DTN zip file"""
    from services.symbol_importer import SymbolImporter
    importer = SymbolImporter(manager)

    # Create a temporary directory to store the uploaded file
    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.join(temp_dir, file.filename)

    try:
        # Save the uploaded file content to a temporary file on the server
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Now, pass the path of the *actual server-side file* to the importer
        results = importer.import_from_dtn_zip(file_path)
        return {"status": "success", "imported": results}
    except Exception as e:
        # Provide a more specific error message
        raise HTTPException(status_code=400, detail=f"Error processing file: {e}")
    finally:
        # Always clean up the temporary file
        if os.path.exists(file_path):
            os.remove(file_path)
