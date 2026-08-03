"""
CDC Archival Handler

Consumes NEW_AND_OLD_IMAGES records from the FinancialLedger DynamoDB Stream,
extracts completed transaction (audit) records, converts them to a columnar
schema, and writes Snappy-compressed Parquet files to S3, partitioned by date:

  s3://<bucket>/year=YYYY/month=MM/day=DD/batch-<uuid>.parquet

Batch item failures are reported back to the event source mapping so that
Lambda retries only the failed records (bisect-on-error is enabled on the
event source mapping in the CDK stack); after exhausting retries, records
are routed to an SQS dead-letter queue.
"""
import io
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()
tracer = Tracer(auto_patch=os.environ.get("POWERTOOLS_TRACE_DISABLED", "").lower() != "true")
metrics = Metrics()

AUDIT_BUCKET_NAME = os.environ["AUDIT_BUCKET_NAME"]
s3_client = boto3.client("s3")


def _decimal_to_number(value: Any) -> Any:
    if isinstance(value, Decimal):
        # Preserve integer vs. float semantics where possible.
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def _deserialize_image(image: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a DynamoDB stream 'NewImage' (low-level typed dict) to a plain dict."""
    from boto3.dynamodb.types import TypeDeserializer

    deserializer = TypeDeserializer()
    return {k: _decimal_to_number(deserializer.deserialize(v)) for k, v in image.items()}


def _extract_audit_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter stream records down to completed audit/event entries and flatten them."""
    audit_records = []
    for record in records:
        dynamodb_payload = record.get("dynamodb", {})
        new_image = dynamodb_payload.get("NewImage")
        if not new_image:
            continue

        item = _deserialize_image(new_image)
        pk = item.get("PK", "")
        status = item.get("status")

        if not pk.startswith("EVENT#") or status != "COMPLETED":
            continue

        audit_records.append(
            {
                "event_id": pk.replace("EVENT#", "", 1),
                "status": status,
                "timestamp": item.get("timestamp"),
                "amount": item.get("amount"),
                "currency": item.get("currency"),
                "source_account": item.get("source_account"),
                "destination_account": item.get("destination_account"),
                "stream_event_name": record.get("eventName"),
                "stream_sequence_number": dynamodb_payload.get("SequenceNumber"),
            }
        )
    return audit_records


def _write_parquet_to_s3(audit_records: List[Dict[str, Any]]) -> Optional[str]:
    if not audit_records:
        return None

    df = pd.DataFrame(audit_records)
    table_arrow = pa.Table.from_pandas(df, preserve_index=False)

    buffer = io.BytesIO()
    pq.write_table(table_arrow, buffer, compression="snappy")
    buffer.seek(0)

    now = datetime.now(timezone.utc)
    key = (
        f"year={now.strftime('%Y')}/month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/batch-{uuid.uuid4()}.parquet"
    )

    s3_client.put_object(
        Bucket=AUDIT_BUCKET_NAME,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
        ServerSideEncryption="AES256",
    )
    return key


@logger.inject_lambda_context(log_event=False)
@tracer.capture_lambda_handler
@metrics.log_metrics
def handler(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    records = event.get("Records", [])
    batch_item_failures: List[Dict[str, str]] = []

    logger.info("Processing DynamoDB stream batch", extra={"record_count": len(records)})

    try:
        audit_records = _extract_audit_records(records)
        object_key = _write_parquet_to_s3(audit_records)

        if object_key:
            logger.info(
                "Wrote Parquet archive to S3",
                extra={"key": object_key, "record_count": len(audit_records)},
            )
            metrics.add_metric(name="CdcRecordsArchived", unit=MetricUnit.Count, value=len(audit_records))
        else:
            logger.info("No completed audit records in this batch; nothing to archive")

    except Exception:
        # If the whole-batch write fails, report every record as failed so the
        # event source mapping retries the batch (bisect-on-error will narrow
        # down the specific poison record on subsequent attempts).
        logger.exception("Failed to archive CDC batch to S3")
        metrics.add_metric(name="CdcArchivalFailureCount", unit=MetricUnit.Count, value=1)
        batch_item_failures = [
            {"itemIdentifier": r["dynamodb"]["SequenceNumber"]}
            for r in records
            if r.get("dynamodb", {}).get("SequenceNumber")
        ]

    return {"batchItemFailures": batch_item_failures}
