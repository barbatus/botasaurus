from .fastapi_app import app as fastapi_app


def run_server():
    import uvicorn

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
