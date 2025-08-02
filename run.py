#!/usr/bin/env python3
"""
Run the FastAPI application with Uvicorn
"""
import os
import sys
import uvicorn
from app.main import app

def setup_debugging():
    """Setup debugging if enabled"""
    debug_mode = os.getenv("DEBUG_MODE", "false").lower() == "true"
    debug_port = int(os.getenv("DEBUG_PORT", "5678"))
    
    if debug_mode:
        try:
            import debugpy
            print(f"🐛 Debug mode enabled - waiting for debugger on port {debug_port}")
            debugpy.listen(("0.0.0.0", debug_port))
            
            # Wait for debugger to attach if WAIT_FOR_DEBUGGER is set
            if os.getenv("WAIT_FOR_DEBUGGER", "false").lower() == "true":
                print("⏳ Waiting for debugger to attach...")
                debugpy.wait_for_client()
                print("✅ Debugger attached!")
        except ImportError:
            print("⚠️  debugpy not installed. Install with: pip install debugpy")
        except Exception as e:
            print(f"❌ Debug setup failed: {e}")

if __name__ == "__main__":
    # Setup debugging if enabled
    setup_debugging()
    
    # Get configuration from environment variables
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload = os.getenv("UVICORN_RELOAD", "true").lower() == "true"
    log_level = os.getenv("LOG_LEVEL", "info")
    workers = int(os.getenv("WORKERS", "1"))
    
    print(f"🚀 Starting FastAPI application on {host}:{port}")
    print(f"📝 Log level: {log_level}")
    print(f"🔄 Auto-reload: {reload}")
    print(f"👥 Workers: {workers}")
    
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level,
        workers=workers if not reload else 1  # Workers > 1 incompatible with reload
    )