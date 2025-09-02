from temporalio import common
from temporalio.client import Client

from botasaurus_server.server import Server

workflow_retry_policy = common.RetryPolicy(
    maximum_attempts=1,
    non_retryable_error_types=["NonRetryableError"],
)


async def get_temporal_client():
    client = await Client.connect(
        Server.temporal_config.url,
        api_key=Server.temporal_config.api_key,
        namespace=Server.temporal_config.namespace,
        tls=Server.temporal_config.tls,
    )
    return client


async def run_scrape_workflow(task_ids: list[int]) -> None:
    from uuid import uuid4

    from .temporal_client import get_temporal_client

    client = await get_temporal_client()
    return await client.start_workflow(
        "runScrapeTasks",
        id=uuid4().hex,
        task_queue="scraper-tasks",
        args=[task_ids],
        retry_policy=workflow_retry_policy,
    )
