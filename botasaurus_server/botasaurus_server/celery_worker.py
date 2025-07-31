
from celery import Celery
from .server import Server

celery_app = Celery(
    "worker",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

celery_app.conf.update(
    worker_concurrency=int(Server.rate_limit["browser"]),

    worker_prefetch_multiplier=1,

    task_always_eager=False,
    task_ignore_result=False,

    task_soft_time_limit=3600,
    task_time_limit=3900,

    worker_pool='threads',

    result_expires=3600,
)
