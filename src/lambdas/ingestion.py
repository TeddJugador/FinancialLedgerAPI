"""
Ingestion & Auth Handler

Receives POST /v1/webhooks/payment from an external payment provider,
verifies the HMAC-SHA256 signature, enforces idempotency, and executes an
atomic double-entry ledger update via DynamoDB TransactWriteItems.
"""
import json
import os
import time
from decimal import Decimal
from typing import Any, Dict, Optional

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from botocore.exceptions import ClientError

from common.signature import is_valid_signature

logger = Logger()
tracer = Tracer(auto_patch=os.environ.get("POWERTOOLS_TRACE_DISABLED", "").lower() != "true")
metrics = Metrics()

TABLE_NAME = os.environ["TABLE_NAME"]
SECRET_ARN = os.environ["SECRET_ARN"]

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
dynamodb_client = boto3.client("dynamodb")
secrets_client = boto3.client("secretsmanager")

# Module-level cache so the secret survives warm invocations (cold-start cache).
_SIGNING_KEY_CACHE: Optional[str] = None

SIGNATURE_HEADER_CANDIDATES = ("x-signature-256", "stripe-signature")


def _get_signing_key() -> str:
    global _SIGNING_KEY_CACHE
    if _SIGNING_KEY_CACHE is not None:
        return _SIGNING_KEY_CACHE

    response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
    secret_payload = json.loads(response["SecretString"])
    _SIGNING_KEY_CACHE = secret_payload["signing_key"]
    return _SIGNING_KEY_CACHE


def _response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _extract_signature_header(headers: Dict[str, str]) -> Optional[str]:
    normalized = {k.lower(): v for k, v in (headers or {}).items()}
    for candidate in SIGNATURE_HEADER_CANDIDATES:
        if candidate in normalized:
            return normalized[candidate]
    return None


def _event_exists(event_id: str) -> bool:
    result = table.get_item(Key={"PK": f"EVENT#{event_id}", "SK": f"EVENT#{event_id}"})
    return "Item" in result


@tracer.capture_method
def _process_transaction(payload: Dict[str, Any]) -> None:
    event_id = payload["event_id"]
    amount = Decimal(str(payload["amount"]))
    currency = payload["currency"]
    source_account = payload["source_account"]
    destination_account = payload["destination_account"]
    timestamp = int(time.time())

    dynamodb_client.transact_write_items(
        TransactItems=[
            {
                "Put": {
                    "TableName": TABLE_NAME,
                    "Item": {
                        "PK": {"S": f"EVENT#{event_id}"},
                        "SK": {"S": f"EVENT#{event_id}"},
                        "GSI1PK": {"S": "STATUS#COMPLETED"},
                        "GSI1SK": {"S": f"TS#{timestamp}"},
                        "status": {"S": "COMPLETED"},
                        "timestamp": {"N": str(timestamp)},
                        "amount": {"N": str(amount)},
                        "currency": {"S": currency},
                        "source_account": {"S": source_account},
                        "destination_account": {"S": destination_account},
                    },
                    "ConditionExpression": "attribute_not_exists(PK)",
                }
            },
            {
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        "PK": {"S": f"ACCOUNT#{source_account}"},
                        "SK": {"S": "METADATA"},
                    },
                    "UpdateExpression": "SET balance = balance - :amount",
                    "ConditionExpression": "balance >= :amount",
                    "ExpressionAttributeValues": {":amount": {"N": str(amount)}},
                }
            },
            {
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        "PK": {"S": f"ACCOUNT#{destination_account}"},
                        "SK": {"S": "METADATA"},
                    },
                    "UpdateExpression": "SET balance = balance + :amount",
                    "ExpressionAttributeValues": {":amount": {"N": str(amount)}},
                }
            },
        ]
    )


REQUIRED_FIELDS = (
    "event_id",
    "amount",
    "currency",
    "source_account",
    "destination_account",
)


def _validate_payload(payload: Dict[str, Any]) -> Optional[str]:
    missing = [field for field in REQUIRED_FIELDS if field not in payload]
    if missing:
        return f"Missing required field(s): {', '.join(missing)}"
    try:
        amount = Decimal(str(payload["amount"]))
    except Exception:
        return "Field 'amount' must be numeric"
    if amount <= 0:
        return "Field 'amount' must be positive"
    return None


@logger.inject_lambda_context(log_event=False)
@tracer.capture_lambda_handler
@metrics.log_metrics
def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start_time = time.time()
    raw_body: str = event.get("body", "") or ""
    if event.get("isBase64Encoded"):
        import base64

        raw_body_bytes = base64.b64decode(raw_body)
    else:
        raw_body_bytes = raw_body.encode("utf-8")

    headers = event.get("headers", {}) or {}
    signature = _extract_signature_header(headers)

    try:
        signing_key = _get_signing_key()
    except ClientError as exc:
        logger.exception("Failed to retrieve webhook signing secret")
        raise exc

    if not is_valid_signature(raw_body_bytes, signing_key, signature or ""):
        logger.warning("Webhook signature verification failed")
        metrics.add_metric(name="TransactionFailureCount", unit=MetricUnit.Count, value=1)
        return _response(401, {"error": "Unauthorized: invalid signature"})

    try:
        payload = json.loads(raw_body_bytes)
    except json.JSONDecodeError:
        logger.warning("Malformed JSON payload received")
        metrics.add_metric(name="TransactionFailureCount", unit=MetricUnit.Count, value=1)
        return _response(400, {"error": "Bad Request: malformed JSON payload"})

    validation_error = _validate_payload(payload)
    if validation_error:
        logger.warning("Payload validation failed", extra={"reason": validation_error})
        metrics.add_metric(name="TransactionFailureCount", unit=MetricUnit.Count, value=1)
        return _response(400, {"error": f"Bad Request: {validation_error}"})

    event_id = payload["event_id"]

    if _event_exists(event_id):
        logger.info("Duplicate event received, short-circuiting", extra={"event_id": event_id})
        metrics.add_metric(name="TransactionSuccessCount", unit=MetricUnit.Count, value=1)
        return _response(200, {"status": "already_processed", "event_id": event_id})

    try:
        _process_transaction(payload)
    except dynamodb_client.exceptions.TransactionCanceledException as exc:
        logger.warning("Transaction cancelled", extra={"reason": str(exc)})
        metrics.add_metric(name="TransactionFailureCount", unit=MetricUnit.Count, value=1)
        return _response(
            422,
            {
                "error": "Unprocessable Entity: transaction failed "
                "(insufficient balance or duplicate event)"
            },
        )
    except ClientError:
        logger.exception("Unexpected error processing transaction")
        metrics.add_metric(name="TransactionFailureCount", unit=MetricUnit.Count, value=1)
        return _response(500, {"error": "Internal Server Error"})

    latency_ms = (time.time() - start_time) * 1000
    metrics.add_metric(name="WebhookIngestionLatency", unit=MetricUnit.Milliseconds, value=latency_ms)
    metrics.add_metric(name="TransactionSuccessCount", unit=MetricUnit.Count, value=1)

    logger.info("Transaction processed successfully", extra={"event_id": event_id})
    return _response(200, {"status": "completed", "event_id": event_id})
