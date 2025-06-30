import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import os

from routers import symbol_router, schedule_router, monitor_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

app = FastAPI(
    title="Ingestion System Admin Dashboard",
    description="Admin interface for managing symbols, schedules, and monitoring data ingestion",
    version="1.0.0"
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8001", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Static files
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
app.mount("/static", StaticFiles(directory=os.path.join(frontend_dir, "static")), name="static")

# Include routers
app.include_router(symbol_router.router, prefix="/api/symbols", tags=["Symbols"])
app.include_router(schedule_router.router, prefix="/api/schedules", tags=["Schedules"])
app.include_router(monitor_router.router, prefix="/api/monitor", tags=["Monitoring"])

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the admin dashboard"""
    index_path = os.path.join(frontend_dir, "index.html")
    with open(index_path, "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "ingestion-admin"}