from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime

router = APIRouter(
    prefix="/api/jobs",
    tags=["jobs"]
)

# Pydantic models for request/response
class JobTriggerRequest(BaseModel):
    job_name: str
    parameters: Optional[Dict[str, Any]] = {}

class JobResponse(BaseModel):
    job_id: str
    job_name: str
    status: str
    triggered_at: str
    message: str

# Example: Simple job status storage (in production, use Redis or database)
job_status_store = {}

# Background task example
def run_trading_job(job_id: str, job_name: str, params: dict):
    """
    This function runs in background.
    Replace with your actual job logic.
    """
    try:
        job_status_store[job_id] = "running"
        
        # Your actual job logic here
        # Example: from src.pipelines import some_pipeline
        # some_pipeline.run(params)
        
        print(f"Job {job_name} started with params: {params}")
        # Simulate work
        import time
        time.sleep(2)
        
        job_status_store[job_id] = "completed"
        print(f"Job {job_name} completed")
    except Exception as e:
        job_status_store[job_id] = f"failed: {str(e)}"
        print(f"Job {job_name} failed: {e}")


# Trigger a job
@router.post("/trigger", response_model=JobResponse)
async def trigger_job(request: JobTriggerRequest, background_tasks: BackgroundTasks):
    """
    Trigger a trading job to run in background
    
    Example:
    POST /api/jobs/trigger
    {
        "job_name": "update_market_data",
        "parameters": {"symbols": ["BTC", "ETH"]}
    }
    """
    job_id = f"{request.job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Add job to background tasks
    background_tasks.add_task(
        run_trading_job, 
        job_id=job_id,
        job_name=request.job_name,
        params=request.parameters
    )
    
    job_status_store[job_id] = "triggered"
    
    return JobResponse(
        job_id=job_id,
        job_name=request.job_name,
        status="triggered",
        triggered_at=datetime.now().isoformat(),
        message=f"Job {request.job_name} has been triggered"
    )


# Get job status
@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """
    Get the status of a triggered job
    """
    if job_id not in job_status_store:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return {
        "job_id": job_id,
        "status": job_status_store[job_id],
        "checked_at": datetime.now().isoformat()
    }


# List all jobs
@router.get("/list")
async def list_jobs():
    """
    List all triggered jobs and their statuses
    """
    return {
        "total_jobs": len(job_status_store),
        "jobs": [
            {"job_id": job_id, "status": status}
            for job_id, status in job_status_store.items()
        ]
    }


# Example: Specific job endpoints
@router.post("/market-data/update")
async def update_market_data(background_tasks: BackgroundTasks, symbols: Optional[list] = None):
    """
    Trigger market data update job
    """
    job_id = f"market_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    background_tasks.add_task(
        run_trading_job,
        job_id=job_id,
        job_name="market_data_update",
        params={"symbols": symbols or ["BTC", "ETH", "SOL"]}
    )
    
    return {
        "job_id": job_id,
        "message": "Market data update triggered",
        "symbols": symbols or ["BTC", "ETH", "SOL"]
    }


@router.post("/strategy/backtest")
async def run_backtest(background_tasks: BackgroundTasks, strategy: str, start_date: str, end_date: str):
    """
    Trigger strategy backtest job
    """
    job_id = f"backtest_{strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    background_tasks.add_task(
        run_trading_job,
        job_id=job_id,
        job_name="backtest",
        params={
            "strategy": strategy,
            "start_date": start_date,
            "end_date": end_date
        }
    )
    
    return {
        "job_id": job_id,
        "message": f"Backtest for {strategy} triggered",
        "parameters": {
            "strategy": strategy,
            "start_date": start_date,
            "end_date": end_date
        }
    }