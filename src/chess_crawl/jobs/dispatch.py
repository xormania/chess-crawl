"""Local polling and SQS delivery are hints; PostgreSQL owns execution rights."""
from __future__ import annotations

import json
import time
import argparse
import importlib
import os
import signal
import threading
from collections.abc import Callable
from typing import Any, Protocol, cast

from chess_crawl.storage.db import Connection, transaction, connection, database_url
from chess_crawl.jobs.models import PROCESSING_JOB_KINDS
from chess_crawl.storage.execution import (
    delivered_dispatch, failed_dispatch, job_dispatch_state, pending_dispatch,
)


class SqsClient(Protocol):
    def send_message(self, **kwargs: Any) -> Any: ...
    def receive_message(self, **kwargs: Any) -> Any: ...
    def delete_message(self, **kwargs: Any) -> Any: ...


def aws_sqs_client() -> SqsClient:
    """Use the SDK's credential chain, including ECS task roles."""
    try:
        boto3 = importlib.import_module("boto3")
    except ImportError:
        raise RuntimeError("Install chess-crawl with the s3 extra for SQS") from None
    return cast(SqsClient, boto3.client("sqs"))


class SqsDispatcher:
    def __init__(self, client: SqsClient, queue_url: str, *, clock: Callable[[], float] = time.time,
                 acquisition_queue_url: str | None = None, processing_queue_url: str | None = None) -> None:
        if bool(acquisition_queue_url) != bool(processing_queue_url):
            raise ValueError("Configure both acquisition and processing queues for stage routing")
        self.client, self.queue_url, self.clock = client, queue_url, clock
        self.acquisition_queue_url = acquisition_queue_url
        self.processing_queue_url = processing_queue_url

    def publish_one(self, conn: Connection) -> bool:
        # Hold only this outbox row. A crash after send and before commit may
        # redeliver; database job ownership makes that duplicate harmless.
        with transaction(conn):
            row = pending_dispatch(conn, now=self.clock())
            if row is None:
                return False
            try:
                self.client.send_message(
                    QueueUrl=(self.processing_queue_url if row["kind"] in PROCESSING_JOB_KINDS
                              else self.acquisition_queue_url) or self.queue_url,
                    MessageBody=json.dumps({"job_id": int(row["job_id"])}),
                )
            except Exception as exc:
                failed_dispatch(conn, int(row["id"]), now=self.clock(), error=type(exc).__name__)
                return False
            delivered_dispatch(conn, int(row["id"]), now=self.clock())
            return True


class SqsConsumer:
    def __init__(self, client: SqsClient, queue_url: str, *, wait_seconds: int = 20) -> None:
        if not 0 <= wait_seconds <= 20:
            raise ValueError("SQS wait_seconds must be between zero and twenty")
        self.client, self.queue_url, self.wait_seconds = client, queue_url, wait_seconds

    def run_once(self, conn: Connection, execute: Callable[[int], int]) -> int:
        response = self.client.receive_message(
            QueueUrl=self.queue_url, MaxNumberOfMessages=1,
            WaitTimeSeconds=self.wait_seconds,
        )
        messages = response.get("Messages", [])
        if not messages:
            return 0
        message = messages[0]
        try:
            payload = json.loads(message["Body"])
            job_id = payload["job_id"]
            if type(job_id) is not int or not 0 < job_id < 2**63:
                raise ValueError("Invalid job id")
        except (KeyError, ValueError, TypeError):
            # Malformed messages are left for the queue's configured DLQ.
            return 0
        claimed = execute(job_id)
        current = job_dispatch_state(conn, job_id)
        # Successful processing has atomically scheduled any retry/continuation
        # in the outbox. Duplicate terminal deliveries need no further work.
        if claimed or current is None or current["state"] in {"done", "error", "skipped", "in_progress"}:
            self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"])
        return claimed


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish durable job dispatch requests to SQS")
    parser.add_argument("--database-url")
    parser.add_argument("--queue-url", default=os.getenv("CHESS_CRAWL_SQS_QUEUE_URL"))
    parser.add_argument("--acquisition-queue-url", default=os.getenv("CHESS_CRAWL_SQS_ACQUISITION_QUEUE_URL"))
    parser.add_argument("--processing-queue-url", default=os.getenv("CHESS_CRAWL_SQS_PROCESSING_QUEUE_URL"))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if not args.queue_url:
        parser.error("Set --queue-url or CHESS_CRAWL_SQS_QUEUE_URL")
    stop = threading.Event()
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    for sig in previous:
        signal.signal(sig, lambda signum, frame: stop.set())
    try:
        dispatcher = SqsDispatcher(aws_sqs_client(), args.queue_url,
                                   acquisition_queue_url=args.acquisition_queue_url,
                                   processing_queue_url=args.processing_queue_url)
        with connection(database_url(args.database_url), mode="rw") as conn:
            while not stop.is_set():
                published = dispatcher.publish_one(conn)
                if args.once:
                    break
                if not published:
                    stop.wait(1)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
