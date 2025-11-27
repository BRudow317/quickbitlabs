"""
main.py - FastAPI application for QuickBitLabs hosting Origin on Q Drive.
Cloudflare Tunnel QuickBitLabsToAndromeda 
"""
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
import requests # Imported as requested for external calls

# --- App Configuration ---
app = FastAPI(
    title="QuickBitLabs",
    description="Host Origin on Q Drive",
    version="1.0.0"
)

# Mount static directory
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup Templates
# Ensure the folder Q:\quickbitlabs\templates exists
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Root endpoint serving a simple HTML page using Jinja2.
    """
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "message": "Welcome to QuickBitLabs",
        "phase": "Phase 1: Public Connectivity"
    })

@app.get("/health")
async def health_check():
    """
    Simple Health check for Phase 1.
    """
    return {
        "status": "healthy", 
        "server": "Andromeda Local", 
        "phase": "1 - Connectivity"
    }

@app.get("/debug/ip")
async def get_public_ip():
    """
    Demonstration of the 'requests' package.
    Fetches the server's public IP from an external API.
    Useful to verify your server has outbound internet access.
    """
    try:
        response = requests.get("https://api.ipify.org?format=json", timeout=5)
        return {"public_ip_source": "api.ipify.org", "data": response.json()}
    except Exception as e:
        return {"error": "Could not fetch IP", "details": str(e)}

if __name__ == "__main__":
    # This block allows running 'python main.py' directly for debugging
    uvicorn.run(app, host="127.0.0.1", port=8000)