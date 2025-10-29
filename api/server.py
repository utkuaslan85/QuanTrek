from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

# Import routers
from routes import jobs

# Create FastAPI app
app = FastAPI(
    title="Quantrek Trading API",
    description="API for automated trading system and device communication",
    version="1.0.0"
)

# Include routers
app.include_router(jobs.router)

# Add CORS middleware for cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your devices' IPs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Custom JSONResponse that adds newline
class PrettyJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return super().render(content) + b'\n'

# Health check endpoint
@app.get("/", response_class=PrettyJSONResponse)
async def root():
    return {
        "status": "online",
        "message": "Quantrek Trading API is running",
        "version": "1.0.0"
    }

# Test endpoint to verify network connectivity
@app.get("/health", response_class=PrettyJSONResponse)
async def health_check():
    return {
        "status": "healthy",
        "service": "quantrek-api"
    }

# Example endpoint - you can add more endpoints below
@app.get("/api/test", response_class=PrettyJSONResponse)
async def test_endpoint():
    return {
        "message": "Test endpoint working!",
        "note": "Add your trading endpoints here"
    }


if __name__ == "__main__":
    uvicorn.run(
        "server:app",  # Module:app object
        host="0.0.0.0",  # Bind to all interfaces (including ZeroTier)
        port=8000,
        reload=True,  # 🔥 Auto-reload on file changes
        reload_dirs=["./"],  # Watch current directory and subdirectories
        log_level="info"
    )