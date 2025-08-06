from celery import Celery

celery_app = Celery(
    "worker",
    broker="redis://host.docker.internal:6379/0",
    backend="redis://host.docker.internal:6379/0",
)

celery_app.conf.update(
    worker_concurrency=3,
    worker_prefetch_multiplier=1,
    task_always_eager=False,
    task_ignore_result=False,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_soft_time_limit=840,
    task_time_limit=900,
    worker_pool="prefork",
    result_expires=3600,
    max_tasks_per_child=50,
    health_check_interval=30,
)
