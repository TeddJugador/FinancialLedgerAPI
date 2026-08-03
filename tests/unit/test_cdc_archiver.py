import io

import pandas as pd
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def handler(audit_bucket):
    import cdc_archiver
    return cdc_archiver


def _stream_record(event_id: str, sequence_number: str, status: str = "COMPLETED"):
    return {
        "eventID": sequence_number,
        "eventName": "INSERT",
        "dynamodb": {
            "SequenceNumber": sequence_number,
            "NewImage": {
                "PK": {"S": f"EVENT#{event_id}"},
                "SK": {"S": f"EVENT#{event_id}"},
                "status": {"S": status},
                "timestamp": {"N": "1700000000"},
                "amount": {"N": "150.50"},
                "currency": {"S": "USD"},
                "source_account": {"S": "acct-source"},
                "destination_account": {"S": "acct-dest"},
            },
        },
    }


def _account_update_record():
    """A non-EVENT record (account balance update) that should be filtered out."""
    return {
        "eventID": "seq-account",
        "eventName": "MODIFY",
        "dynamodb": {
            "SequenceNumber": "seq-account",
            "NewImage": {
                "PK": {"S": "ACCOUNT#acct-source"},
                "SK": {"S": "METADATA"},
                "balance": {"N": "750"},
            },
        },
    }


def test_writes_parquet_for_completed_events(handler, audit_bucket, lambda_context):
    import boto3

    event = {
        "Records": [
            _stream_record("evt-1", "seq-1"),
            _account_update_record(),
            _stream_record("evt-2", "seq-2"),
        ]
    }

    result = handler.handler(event, context=lambda_context)

    assert result["batchItemFailures"] == []

    s3 = boto3.client("s3", region_name="us-east-1")
    objects = s3.list_objects_v2(Bucket=audit_bucket).get("Contents", [])
    assert len(objects) == 1

    key = objects[0]["Key"]
    assert key.startswith("year=")
    assert key.endswith(".parquet")

    body = s3.get_object(Bucket=audit_bucket, Key=key)["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    df = table.to_pandas()

    assert len(df) == 2
    assert set(df["event_id"]) == {"evt-1", "evt-2"}
    assert set(df["status"]) == {"COMPLETED"}


def test_ignores_non_completed_and_non_event_records(handler, audit_bucket, lambda_context):
    import boto3

    event = {
        "Records": [
            _account_update_record(),
            _stream_record("evt-pending", "seq-pending", status="PENDING"),
        ]
    }

    result = handler.handler(event, context=lambda_context)

    assert result["batchItemFailures"] == []

    s3 = boto3.client("s3", region_name="us-east-1")
    objects = s3.list_objects_v2(Bucket=audit_bucket).get("Contents", [])
    assert len(objects) == 0


def test_empty_batch_produces_no_writes(handler, audit_bucket, lambda_context):
    import boto3

    result = handler.handler({"Records": []}, context=lambda_context)

    assert result["batchItemFailures"] == []
    s3 = boto3.client("s3", region_name="us-east-1")
    assert "Contents" not in s3.list_objects_v2(Bucket=audit_bucket)
