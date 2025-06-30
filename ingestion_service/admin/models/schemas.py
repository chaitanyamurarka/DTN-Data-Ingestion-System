from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

class ExchangeEnum(str, Enum):
    NYSE = "NYSE"
    NASDAQ = "NASDAQ"
    CME = "CME"
    EUREX = "EUREX"

class SecurityTypeEnum(str, Enum):
    STOCK = "STOCK"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    INDEX = "INDEX"
    FOREX = "FOREX"
    CRYPTO = "CRYPTO"

class ScheduleTypeEnum(str, Enum):
    HISTORICAL = "historical"
    LIVE = "live"

class SymbolBase(BaseModel):
    symbol: str = Field(..., description="Trading symbol")
    exchange: ExchangeEnum = Field(..., description="Exchange")
    security_type: SecurityTypeEnum = Field(..., description="Security type")
    description: str = Field("", description="Symbol description")

class SymbolCreate(SymbolBase):
    historical_days: int = Field(30, ge=1, le=365, description="Days of historical data to fetch")
    backfill_minutes: int = Field(120, ge=0, le=1440, description="Minutes of backfill for live data")
    added_by: str = Field(..., description="User who added the symbol")

class SymbolUpdate(BaseModel):
    description: Optional[str] = None
    historical_days: Optional[int] = Field(None, ge=1, le=365)
    backfill_minutes: Optional[int] = Field(None, ge=0, le=1440)
    active: Optional[bool] = None

class Symbol(SymbolBase):
    id: str
    active: bool = True
    historical_days: int = 30
    backfill_minutes: int = 120
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_ingestion: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class SymbolFilter(BaseModel):
    exchanges: Optional[List[ExchangeEnum]] = None
    security_types: Optional[List[SecurityTypeEnum]] = None
    search_text: Optional[str] = None
    active: Optional[bool] = None
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)

class ScheduleCreate(BaseModel):
    symbol: str
    schedule_type: ScheduleTypeEnum
    cron_expression: str = Field(..., description="Cron expression for schedule")
    enabled: bool = True
    config: Dict[str, Any] = Field(default_factory=dict)

class ScheduleConfig(ScheduleCreate):
    id: str
    created_at: datetime
    updated_at: datetime
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    
class DataAvailability(BaseModel):
    symbol: str
    timeframe: str
    first_timestamp: Optional[datetime]
    last_timestamp: Optional[datetime]
    duration_days: int
    data_points: int
    
class IngestionStats(BaseModel):
    total_symbols: int
    active_symbols: int
    total_data_points: int
    disk_usage_mb: float
    last_update: Optional[datetime]