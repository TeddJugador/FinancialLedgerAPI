#!/usr/bin/env python3
"""
CDK application entrypoint for the Serverless Financial Ledger API &
High-Throughput Webhook Ingestion Service.
"""
import os

import aws_cdk as cdk

from stacks.ledger_stack import LedgerStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

LedgerStack(
    app,
    "FinancialLedgerStack",
    env=env,
    description="Event-driven ledger API with HMAC-verified webhook ingestion "
    "and Parquet CDC archival to S3.",
)

app.synth()
