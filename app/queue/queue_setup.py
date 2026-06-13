"""
Creates Retry and DLQ queues
for all registered queues.
"""

from app.queue.queue_names import ALL_QUEUES


async def declare_retry_dlq_queues(channel):

    for queue in ALL_QUEUES:

        retry_queue = f"{queue}.retry"
        dlq_queue = f"{queue}.dlq"

        await channel.declare_queue(
            retry_queue,
            durable=True
        )

        await channel.declare_queue(
            dlq_queue,
            durable=True
        )

        print(f"Created: {retry_queue}")
        print(f"Created: {dlq_queue}")

    print("All Retry and DLQ queues declared")