import json
import os

import boto3
import pytest
from moto import mock_aws

# Environment variables must be set before the lambda modules are imported,
# since they read os.environ at module load time.
os.environ.setdefault("TABLE_NAME", "FinancialLedger")
os.environ.setdefault("SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret:test-secret")
os.environ.setdefault("AUDIT_BUCKET_NAME", "ledger-audit-archive-test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("POWERTOOLS_SERVICE_NAME", "ledger-test")
os.environ.setdefault("POWERTOOLS_METRICS_NAMESPACE", "FinancialLedgerTest")
# aws_lambda_powertools.Tracer auto-patches boto3 for X-Ray on import, which
# conflicts with moto's request interception. Disable it for the test run.
os.environ.setdefault("POWERTOOLS_TRACE_DISABLED", "true")

TEST_SIGNING_KEY = "unit-test-signing-key"


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def moto_env(aws_credentials):
    with mock_aws():
        yield


@pytest.fixture
def ledger_table(moto_env):
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    table = ddb.create_table(
        TableName=os.environ["TABLE_NAME"],
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()

    # Seed two accounts used across tests. We use the low-level client (typed
    # attribute values) rather than the resource's put_item here: moto's
    # TransactWriteItems arithmetic on numeric attributes written via the
    # resource layer (which stores them as Decimal) can raise a spurious
    # TypeError; the low-level client avoids that quirk.
    low_level = boto3.client("dynamodb", region_name="us-east-1")
    low_level.put_item(
        TableName=os.environ["TABLE_NAME"],
        Item={"PK": {"S": "ACCOUNT#acct-source"}, "SK": {"S": "METADATA"}, "balance": {"N": "1000"}},
    )
    low_level.put_item(
        TableName=os.environ["TABLE_NAME"],
        Item={"PK": {"S": "ACCOUNT#acct-dest"}, "SK": {"S": "METADATA"}, "balance": {"N": "0"}},
    )
    return table


@pytest.fixture
def webhook_secret(moto_env):
    sm = boto3.client("secretsmanager", region_name="us-east-1")
    secret = sm.create_secret(
        Name="test-secret",
        SecretString=json.dumps({"signing_key": TEST_SIGNING_KEY}),
    )
    # Point the module env var at the ARN moto actually generated.
    os.environ["SECRET_ARN"] = secret["ARN"]
    return secret["ARN"]


class _FakeLambdaContext:
    function_name = "test-function"
    memory_limit_in_mb = 128
    invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
    aws_request_id = "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def lambda_context():
    return _FakeLambdaContext()


@pytest.fixture
def audit_bucket(moto_env):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=os.environ["AUDIT_BUCKET_NAME"])
    return os.environ["AUDIT_BUCKET_NAME"]
