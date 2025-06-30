from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
from services.data_monitor import DataMonitor
from models.schemas import DataAvailability, IngestionStats

router = APIRouter()

def get_data_monitor():
    return DataMonitor()

@router.get("/availability/{symbol}", response_model=Dict[str, DataAvailability])
async def get_symbol_availability(
    symbol: str,
    monitor: DataMonitor = Depends(get_data_monitor)
):
    """Get data availability for all timeframes of a symbol"""
    try:
        return monitor.get_symbol_data_availability(symbol)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/statistics", response_model=IngestionStats)
async def get_ingestion_statistics(
    monitor: DataMonitor = Depends(get_data_monitor)
):
    """Get overall ingestion statistics"""
    return monitor.get_ingestion_statistics()

@router.get("/activity")
async def get_recent_activity(
    hours: int = 24,
    monitor: DataMonitor = Depends(get_data_monitor)
):
    """Get recent ingestion activity"""
    return monitor.get_recent_activity(hours)

@router.get("/health/{symbol}")
async def check_symbol_health(
    symbol: str,
    monitor: DataMonitor = Depends(get_data_monitor)
):
    """Check health status of symbol data ingestion"""
    health = monitor.check_symbol_health(symbol)
    if health["status"] == "error":
        raise HTTPException(status_code=503, detail=health["message"])
    return health