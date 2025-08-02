# OrienteeringX API

A FastAPI enterprise-level web API project with MongoDB integration, Swagger UI, and a batch job scheduling system.

## Features

- **FastAPI Framework**: High-performance, easy to learn, fast to code, ready for production
- **MongoDB Integration**: Using Motor for asynchronous database operations
- **Swagger UI**: Interactive API documentation
- **Authentication**: JWT-based authentication system
- **Batch Job System**: Scheduling and execution of batch jobs with parameters
- **Async Support**: Fully asynchronous API endpoints and database operations
- **Dependency Injection**: Clean architecture with dependency injection
- **Environment Configuration**: Environment-based configuration system
- **CORS Middleware**: Cross-Origin Resource Sharing support
- **Pydantic Models**: Data validation and settings management

## Project Structure

```
orienteeringx-api-python/
├── app/
│   ├── api/
│   │   └── v1/
│   │       ├── endpoints/
│   │       │   ├── auth.py
│   │       │   ├── batch_jobs.py
│   │       │   ├── health.py
│   │       │   └── users.py
│   │       └── api.py
│   ├── core/
│   │   ├── config.py
│   │   ├── deps.py
│   │   └── security.py
│   ├── db/
│   │   └── mongodb.py
│   ├── jobs/
│   │   ├── executor.py
│   │   └── scheduler.py
│   ├── models/
│   │   └── models.py
│   ├── schemas/
│   │   └── schemas.py
│   ├── services/
│   │   ├── batch_job_service.py
│   │   └── user_service.py
│   └── main.py
├── .env
├── requirements.txt
└── run.py
```

## Getting Started

### Prerequisites

- Python 3.8+
- MongoDB
- Redis (for Celery, optional)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/orienteeringx-api-python.git
cd orienteeringx-api-python
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
Edit the `.env` file with your specific configuration.

### Running the Application

```bash
python run.py
```

The API will be available at http://localhost:8000

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API Endpoints

### Authentication
- `POST /api/v1/auth/register` - Register a new user
- `POST /api/v1/auth/login` - Login and get access token
- `GET /api/v1/auth/me` - Get current user information

### Users
- `GET /api/v1/users/` - Get list of users (admin only)
- `POST /api/v1/users/` - Create a new user (admin only)
- `GET /api/v1/users/{user_id}` - Get user by ID
- `PUT /api/v1/users/{user_id}` - Update user
- `DELETE /api/v1/users/{user_id}` - Delete user (admin only)

### Batch Jobs
- `GET /api/v1/batch-jobs/` - Get list of batch jobs
- `POST /api/v1/batch-jobs/` - Create a new batch job
- `GET /api/v1/batch-jobs/{job_id}` - Get batch job by ID
- `PUT /api/v1/batch-jobs/{job_id}` - Update batch job
- `DELETE /api/v1/batch-jobs/{job_id}` - Delete batch job
- `GET /api/v1/batch-jobs/types/available` - Get available job types
- `GET /api/v1/batch-jobs/scheduler/status` - Get scheduler status (admin only)

### Health
- `GET /api/v1/health` - Health check endpoint
- `GET /api/v1/` - Root endpoint

## Batch Job System

The batch job system allows scheduling and execution of various job types with custom parameters. Available job types:

- `sample_job` - A sample job for demonstration purposes
- `data_processing` - Process data from various sources
- `email_notification` - Send email notifications
- `report_generation` - Generate various types of reports

To create a custom job type, extend the `JobExecutor` class in `app/jobs/executor.py` and register a new handler.

## License

This project is licensed under the MIT License - see the LICENSE file for details.