import json
from typing import List, Optional, Dict
from datetime import datetime, time
import redis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
from config import settings

from models.schemas import ScheduleConfig, ScheduleCreate

logger = logging.getLogger(__name__)

class ScheduleManager:
    def __init__(self):
        self.redis_client = redis.from_url(settings.REDIS_URL)
        self.scheduler = AsyncIOScheduler()
        self.schedule_key_prefix = "schedule:"
        
    def create_schedule(self, schedule: ScheduleCreate) -> ScheduleConfig:
        """Create or update a schedule configuration"""
        schedule_id = f"{schedule.symbol}_{schedule.schedule_type}"
        
        config = ScheduleConfig(
            id=schedule_id,
            symbol=schedule.symbol,
            schedule_type=schedule.schedule_type,
            cron_expression=schedule.cron_expression,
            enabled=schedule.enabled,
            config=schedule.config,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Store in Redis
        key = f"{self.schedule_key_prefix}{schedule_id}"
        self.redis_client.set(key, config.json())
        
        # Update scheduler if enabled
        if config.enabled:
            self._update_scheduler_job(config)
        
        return config
    
    def get_schedules(self, symbol: Optional[str] = None) -> List[ScheduleConfig]:
        """Get all schedules or schedules for a specific symbol"""
        pattern = f"{self.schedule_key_prefix}*"
        if symbol:
            pattern = f"{self.schedule_key_prefix}{symbol}_*"
        
        schedules = []
        for key in self.redis_client.scan_iter(match=pattern):
            data = self.redis_client.get(key)
            if data:
                schedule = ScheduleConfig.parse_raw(data)
                schedules.append(schedule)
        
        return schedules
    
    def _update_scheduler_job(self, config: ScheduleConfig):
        """Update APScheduler job based on config"""
        job_id = f"ingestion_{config.id}"
        
        # Remove existing job if any
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
        
        if config.enabled:
            # Parse cron expression
            cron_parts = config.cron_expression.split()
            trigger = CronTrigger(
                minute=cron_parts[0],
                hour=cron_parts[1],
                day=cron_parts[2],
                month=cron_parts[3],
                day_of_week=cron_parts[4]
            )
            
            # Add job based on type
            if config.schedule_type == "historical":
                self.scheduler.add_job(
                    self._run_historical_ingestion,
                    trigger=trigger,
                    id=job_id,
                    args=[config.symbol, config.config]
                )
            elif config.schedule_type == "live":
                self.scheduler.add_job(
                    self._run_live_ingestion,
                    trigger=trigger,
                    id=job_id,
                    args=[config.symbol, config.config]
                )
    
    async def _run_historical_ingestion(self, symbol: str, config: dict):
        """Trigger historical data ingestion for a symbol"""
        # This will call the modified ohlc_ingest.py
        logger.info(f"Running historical ingestion for {symbol} with config: {config}")
        # Implementation to trigger ohlc_ingest
    
    async def _run_live_ingestion(self, symbol: str, config: dict):
        """Trigger live data ingestion for a symbol"""
        # This will call the modified live_tick_ingest.py
        logger.info(f"Running live ingestion for {symbol} with config: {config}")
        # Implementation to trigger live_tick_ingest