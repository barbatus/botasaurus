import uvicorn
from celery.apps.worker import Worker

# from .env import is_in_kubernetes
from .fastapi_app import app as fastapi_app


def run_server():
    host = "0.0.0.0"

    uvicorn.run(fastapi_app, host=host, port=8000, log_level="info")


def run_worker():
    from .celery_worker import celery_app

    worker_instance = Worker(
        app=celery_app,
        loglevel="INFO",
        traceback=True,
        pool="prefork",
        autoscale="5,1",
    )
    worker_instance.start()
