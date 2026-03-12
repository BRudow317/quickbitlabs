import uvicorn
from fastapi.staticfiles import StaticFiles
from server.app import create_app
import os

app = create_app()

# Serve the static files from the root dist/ folder
# This is where Vite builds your React app
if os.path.exists("dist"):
    app.mount("/", StaticFiles(directory="dist", html=True), name="static")

if __name__ == "__main__":
    # In development, we use reload. In production, we don't.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)