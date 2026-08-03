import hashlib
import hmac
import json

import pytest

from conftest import TEST_SIGNING_KEY


def _sign(body: str, key: str = TEST_SIGNING_KEY) -> str:
    return hmac.new(key.encode(), body.encode(), hashlib.sha256).hexdigest()


def _build_event(body: dict, signature: str, is_base64: bool = False):
    raw_body = json.dumps(body)
    return {
        "body": raw_body,
        "isBase64Encoded": is_base64,
        "headers": {"X-Signature-256": signature},
    }


@pytest.fixture
def handler(ledger_table, webhook_secret):
    # Import after fixtures configure env vars / moto so the module picks up
    # the mocked resources, and reset the module-level secret cache each test.
    import ingestion
    ingestion._SIGNING_KEY_CACHE = None
    # The module-level SECRET_ARN was captured at import time; keep it in sync
    # with the ARN moto generated for this test's secret.
    ingestion.SECRET_ARN = webhook_secret
    return ingestion


def test_invalid_signature_returns_401(handler, lambda_context):
    body = {
        "event_id": "evt-1",
        "amount": "100.00",
        "currency": "USD",
        "source_account": "acct-source",
        "destination_account": "acct-dest",
    }
    event = _build_event(body, signature="deadbeef")

    response = handler.handler(event, context=lambda_context)

    assert response["statusCode"] == 401
    assert "Unauthorized" in json.loads(response["body"])["error"]


def test_malformed_json_returns_400(handler, lambda_context):
    raw_body = "{not valid json"
    event = {
        "body": raw_body,
        "isBase64Encoded": False,
        "headers": {"X-Signature-256": _sign(raw_body)},
    }

    response = handler.handler(event, context=lambda_context)

    assert response["statusCode"] == 400
    assert "malformed JSON" in json.loads(response["body"])["error"]


def test_successful_transaction_updates_balances(handler, ledger_table, lambda_context):
    body = {
        "event_id": "evt-2",
        "amount": "250.00",
        "currency": "USD",
        "source_account": "acct-source",
        "destination_account": "acct-dest",
    }
    event = _build_event(body, signature=_sign(json.dumps(body)))

    response = handler.handler(event, context=lambda_context)

    assert response["statusCode"] == 200
    assert json.loads(response["body"])["status"] == "completed"

    source = ledger_table.get_item(Key={"PK": "ACCOUNT#acct-source", "SK": "METADATA"})["Item"]
    dest = ledger_table.get_item(Key={"PK": "ACCOUNT#acct-dest", "SK": "METADATA"})["Item"]
    assert source["balance"] == 750
    assert dest["balance"] == 250

    event_record = ledger_table.get_item(Key={"PK": "EVENT#evt-2", "SK": "EVENT#evt-2"})["Item"]
    assert event_record["status"] == "COMPLETED"


def test_duplicate_event_is_idempotent(handler, ledger_table, lambda_context):
    body = {
        "event_id": "evt-3",
        "amount": "50.00",
        "currency": "USD",
        "source_account": "acct-source",
        "destination_account": "acct-dest",
    }
    event = _build_event(body, signature=_sign(json.dumps(body)))

    first = handler.handler(event, context=lambda_context)
    second = handler.handler(event, context=lambda_context)

    assert first["statusCode"] == 200
    assert second["statusCode"] == 200
    assert json.loads(second["body"])["status"] == "already_processed"

    # Balance should reflect only ONE application of the debit/credit.
    source = ledger_table.get_item(Key={"PK": "ACCOUNT#acct-source", "SK": "METADATA"})["Item"]
    assert source["balance"] == 950


def test_insufficient_balance_rolls_back_atomically(handler, ledger_table, lambda_context):
    """
    Integration-style test: verifies that when the debit leg's condition
    expression fails (insufficient balance), the ENTIRE TransactWriteItems
    call is rolled back — no partial credit, and no audit record is written.
    """
    body = {
        "event_id": "evt-4",
        "amount": "999999.00",  # exceeds the seeded 1000 balance
        "currency": "USD",
        "source_account": "acct-source",
        "destination_account": "acct-dest",
    }
    event = _build_event(body, signature=_sign(json.dumps(body)))

    response = handler.handler(event, context=lambda_context)

    assert response["statusCode"] == 422
    assert "Unprocessable Entity" in json.loads(response["body"])["error"]

    # Confirm atomic rollback: source/dest balances untouched, no audit item.
    source = ledger_table.get_item(Key={"PK": "ACCOUNT#acct-source", "SK": "METADATA"})["Item"]
    dest = ledger_table.get_item(Key={"PK": "ACCOUNT#acct-dest", "SK": "METADATA"})["Item"]
    assert source["balance"] == 1000
    assert dest["balance"] == 0

    assert "Item" not in ledger_table.get_item(Key={"PK": "EVENT#evt-4", "SK": "EVENT#evt-4"})


def test_missing_required_field_returns_400(handler, lambda_context):
    body = {
        "event_id": "evt-5",
        "amount": "10.00",
        "currency": "USD",
        # source_account intentionally omitted
        "destination_account": "acct-dest",
    }
    event = _build_event(body, signature=_sign(json.dumps(body)))

    response = handler.handler(event, context=lambda_context)

    assert response["statusCode"] == 400
    assert "source_account" in json.loads(response["body"])["error"]
