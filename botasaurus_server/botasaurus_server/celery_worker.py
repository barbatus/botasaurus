from celery import Celery

celery_app = Celery(
    "worker",
    broker="redis://host.docker.internal:6379/0",
    backend="redis://host.docker.internal:6379/0",
)

celery_app.conf.update(
    task_always_eager=False,
    task_ignore_result=False,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_soft_time_limit=240,
    task_time_limit=300,
    worker_pool="prefork",
    result_expires=3600,
    worker_max_tasks_per_child=50,
    redis_backend_health_check_interval=60,
    worker_prefetch_multiplier=1,
    broker_transport_options={
        "visibility_timeout": 7200,
        "socket_timeout": 30,
        "socket_connect_timeout": 30,
        "health_check_interval": 30,
        "retry_on_timeout": True,
        "socket_keepalive": True,
    },
    result_backend_transport_options={
        "health_check_interval": 30,
        "retry_on_timeout": True,
    },
)
