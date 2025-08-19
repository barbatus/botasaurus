# from .env import is_in_kubernetes


def run_server():
    import uvicorn

    from .fastapi_app import app as fastapi_app

    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000, log_level="info")


def run_worker():
    from celery.apps.worker import Worker

    from .celery_worker import celery_app

    worker_instance = Worker(
        app=celery_app,
        loglevel="INFO",
        traceback=True,
        pool="prefork",
        autoscale="5,3",
    )
    worker_instance.start()
