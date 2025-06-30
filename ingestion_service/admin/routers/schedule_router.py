from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
from services.schedule_manager import ScheduleManager
from models.schemas import ScheduleConfig, ScheduleCreate

router = APIRouter()

def get_schedule_manager():
    return ScheduleManager()

@router.post("/", response_model=ScheduleConfig)
async def create_schedule(
    schedule: ScheduleCreate,
    manager: ScheduleManager = Depends(get_schedule_manager)
):
    """Create or update a schedule"""
    try:
        return manager.create_schedule(schedule)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/", response_model=List[ScheduleConfig])
async def get_schedules(
    symbol: Optional[str] = None,
    manager: ScheduleManager = Depends(get_schedule_manager)
):
    """Get all schedules or schedules for a specific symbol"""
    return manager.get_schedules(symbol)

@router.get("/{schedule_id}", response_model=ScheduleConfig)
async def get_schedule(
    schedule_id: str,
    manager: ScheduleManager = Depends(get_schedule_manager)
):
    """Get a specific schedule"""
    schedule = manager.get_schedule(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return schedule

@router.patch("/{schedule_id}/toggle")
async def toggle_schedule(
    schedule_id: str,
    manager: ScheduleManager = Depends(get_schedule_manager)
):
    """Enable/disable a schedule"""
    if not manager.toggle_schedule(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"status": "success"}

@router.delete("/{schedule_id}")
async def delete_schedule(
    schedule_id: str,
    manager: ScheduleManager = Depends(get_schedule_manager)
):
    """Delete a schedule"""
    if not manager.delete_schedule(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"status": "success"}

@router.post("/{schedule_id}/run")
async def run_schedule_now(
    schedule_id: str,
    manager: ScheduleManager = Depends(get_schedule_manager)
):
    """Manually trigger a schedule"""
    try:
        await manager.run_schedule_now(schedule_id)
        return {"status": "success", "message": f"Schedule {schedule_id} triggered"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))