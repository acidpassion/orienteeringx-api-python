# 🐛 Debugging Guide for OrienteeringX API

This guide provides comprehensive debugging options for the FastAPI application with job scheduling system.

## 🚀 Quick Start Debugging

### 1. VS Code Debugging (Recommended)

Open the project in VS Code and use the debug configurations:

#### Available Debug Configurations:

1. **Debug FastAPI App** - Main application with auto-reload
2. **Debug FastAPI App (No Reload)** - Better for breakpoint debugging
3. **Debug Specific Job** - Debug individual job functions
4. **Debug Setup Sample Data** - Debug database setup scripts
5. **Python Debugger: Current File** - Debug any Python file
6. **Attach to Running FastAPI** - Attach to a running instance

#### How to Use:
1. Press `F5` or go to `Run and Debug` panel
2. Select your desired configuration
3. Set breakpoints in your code
4. Start debugging!

### 2. Command Line Debugging

#### Basic Debugging:
```bash
# Activate virtual environment
source venv/bin/activate

# Run with debug mode
DEBUG_MODE=true python run.py
```

#### Advanced Debugging Options:
```bash
# Debug with custom port
DEBUG_MODE=true DEBUG_PORT=5679 python run.py

# Wait for debugger to attach
DEBUG_MODE=true WAIT_FOR_DEBUGGER=true python run.py

# Custom host and port
HOST=127.0.0.1 PORT=8001 python run.py

# Disable auto-reload for stable debugging
UVICORN_RELOAD=false python run.py

# Increase log verbosity
LOG_LEVEL=debug python run.py
```

## 🔧 Debugging Specific Components

### FastAPI Application
```python
# Add breakpoints in app/main.py
# Debug startup/shutdown lifecycle
# Monitor API endpoints
```

### Job Scheduler
```python
# Debug in app/jobs/scheduler.py
# Monitor job execution
# Check APScheduler logs
```

### Database Operations
```python
# Debug in app/db/mongodb.py
# Monitor MongoDB connections
# Check collection operations
```

### Scrape Jobs
```python
# Debug in app/jobs/sample_jobs.py
# Monitor data fetching
# Check API responses
```

## 🛠️ Debugging Tools & Techniques

### 1. Logging
```python
import logging
logger = logging.getLogger(__name__)

# Add debug logs
logger.debug("Debug information")
logger.info("General information")
logger.warning("Warning message")
logger.error("Error occurred")
```

### 2. Print Debugging
```python
# Quick debugging with prints
print(f"🐛 DEBUG: variable_name = {variable_name}")
print(f"📊 DATA: {data}")
print(f"⚠️  WARNING: {warning_message}")
```

### 3. Breakpoints
```python
# Python debugger breakpoint
import pdb; pdb.set_trace()

# Or use the built-in breakpoint() function (Python 3.7+)
breakpoint()
```

### 4. Exception Handling
```python
try:
    # Your code here
    pass
except Exception as e:
    logger.error(f"Error occurred: {e}")
    # Optional: re-raise for debugging
    raise
```

## 🌐 Web Debugging

### API Endpoints
- **FastAPI Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Job Dashboard**: http://localhost:8000/jobs/dashboard

### Useful API Endpoints for Debugging:
```bash
# Check application health
curl http://localhost:8000/api/v1/health

# Get job status
curl http://localhost:8000/jobs/api/status

# List all jobs
curl http://localhost:8000/jobs/api/jobs

# Get specific job
curl http://localhost:8000/jobs/api/jobs/{job_id}
```

## 📊 Monitoring & Logs

### Application Logs
```bash
# View real-time logs
tail -f logs/app.log

# Search for errors
grep -i error logs/app.log

# Filter by component
grep "scheduler" logs/app.log
```

### Job Execution Logs
- Check APScheduler logs in the console
- Monitor job execution times
- Watch for failed jobs

### Database Monitoring
```python
# Check MongoDB connection
from app.db.mongodb import get_database

async def check_db():
    db = await get_database()
    collections = await db.list_collection_names()
    print(f"Available collections: {collections}")
```

## 🚨 Common Issues & Solutions

### 1. ModuleNotFoundError in VS Code Debugger
**Error**: `No module named 'uvicorn'` or similar import errors

**Solution**:
- Ensure VS Code is using the correct Python interpreter
- Check that `python.pythonPath` is set to `./venv/bin/python` in VS Code settings
- Restart VS Code after changing interpreter settings
- Verify the debug configuration includes `"python": "${workspaceFolder}/venv/bin/python"`

**Quick Fix**:
```bash
# In VS Code, press Cmd+Shift+P (Mac) or Ctrl+Shift+P (Windows/Linux)
# Type "Python: Select Interpreter"
# Choose the interpreter from ./venv/bin/python
```

### 2. Import Errors
```bash
# Ensure PYTHONPATH is set
export PYTHONPATH=/path/to/project:$PYTHONPATH

# Or use the -m flag
python -m app.main
```

### 3. Database Connection Issues
```python
# Check MongoDB connection
# Verify connection string in .env
# Ensure MongoDB is running
```

### 4. Job Scheduling Issues
```python
# Check APScheduler logs
# Verify job functions are async
# Monitor job store status
```

### 5. Port Already in Use
```bash
# Find process using port 8000
lsof -i :8000

# Kill the process
kill -9 <PID>

# Or use a different port
PORT=8001 python run.py
```

## 🔍 Advanced Debugging

### Remote Debugging
1. Start app with debug mode:
   ```bash
   DEBUG_MODE=true WAIT_FOR_DEBUGGER=true python run.py
   ```

2. In VS Code, use "Attach to Running FastAPI" configuration

3. Connect to localhost:5678

### Performance Debugging
```python
# Add timing decorators
import time
from functools import wraps

def timing_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        result = await func(*args, **kwargs)
        end = time.time()
        print(f"⏱️  {func.__name__} took {end - start:.2f} seconds")
        return result
    return wrapper
```

### Memory Debugging
```python
# Monitor memory usage
import psutil
import os

def get_memory_usage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return f"Memory: {memory_info.rss / 1024 / 1024:.2f} MB"
```

## 📝 Debugging Checklist

- [ ] Set appropriate log level
- [ ] Add breakpoints at key locations
- [ ] Check environment variables
- [ ] Verify database connections
- [ ] Monitor job execution
- [ ] Test API endpoints
- [ ] Check for memory leaks
- [ ] Validate input data
- [ ] Handle exceptions properly
- [ ] Use debugging tools effectively

## 🆘 Getting Help

1. Check application logs first
2. Use VS Code debugger for step-through debugging
3. Test individual components in isolation
4. Monitor the job dashboard for real-time status
5. Use API documentation for endpoint testing

Happy debugging! 🐛✨