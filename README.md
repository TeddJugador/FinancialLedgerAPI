# Financial Ledger API
*by Theodore Masi*

A small, serverless payment ledger built on AWS. It listens for payment
webhooks, verifies they're authentic, records the transaction safely (no
double-charging, no partial writes), and keeps a permanent audit trail of
everything that happens.

## Why this exists

This started as a standalone portfolio project, but the design was chosen
with a specific end goal in mind: it's meant to become the payments
subsystem for a school management system I'm developing; the piece
responsible for handling tuition/fee payments, verifying that money moved
correctly between accounts, and keeping a clean audit record for the school's
records. The webhook-driven, double-entry design here is built to slot into
that larger system later, not just as an isolated demo.

## How it works, in plain terms

1. A payment provider sends a webhook when money should move from one
   account to another.
2. The service checks that the webhook is genuinely from the payment
   provider (not spoofed).
3. It checks whether this exact payment was already processed before (so a
   retried webhook can't charge someone twice).
4. It updates both account balances *and* writes a permanent record of the
   transaction all in a single atomic operation, so it's impossible to end
   up in a state where money left one account but never arrived at the
   other.
5. In the background, every completed transaction is also copied into a
   compressed, permanent archive file in S3; a second, independent copy of
   the history for auditing, kept separate from the live database.

## Project layout

```
app.py                          Entry point for the AWS infrastructure definition
stacks/ledger_stack.py          Defines every AWS resource this project uses
src/lambdas/ingestion.py        Verifies webhooks and records transactions
src/lambdas/cdc_archiver.py     Copies completed transactions to S3 for archiving
src/lambdas/common/signature.py Shared signature-verification logic
tests/unit/                     Automated tests (run without needing real AWS access)
scripts/generate_hmac.py        Helper for sending a properly signed test request
```

## Setting it up

You'll need:
- Python 3.12+
- Node.js and the AWS CDK CLI (`npm install -g aws-cdk`)
- Docker running locally (used to package the code for deployment)
- An AWS account with credentials configured (`aws configure`)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

## Deploying

```bash
cdk bootstrap   # one-time setup per AWS account/region
cdk deploy
```

This creates everything the service needs: the database, the storage
bucket, the API endpoint, and the two functions that do the actual work.
It'll ask you to confirm before creating anything real in your AWS account.

## Trying it out

Once deployed, find your API's web address:

```bash
aws apigatewayv2 get-apis --region us-east-1 \
  --query "Items[?Name=='financial-ledger-api'].ApiEndpoint" --output text
```

Grab the signing key the service uses to verify requests:

```bash
SIGNING_KEY=$(aws secretsmanager get-secret-value \
  --secret-id ledger/webhook-signing-secret \
  --region us-east-1 \
  --query SecretString --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)["signing_key"])')
```

Create two test accounts to move money between:

```bash
aws dynamodb put-item --table-name FinancialLedger --region us-east-1 \
  --item '{"PK":{"S":"ACCOUNT#acct-source"},"SK":{"S":"METADATA"},"balance":{"N":"1000"}}'

aws dynamodb put-item --table-name FinancialLedger --region us-east-1 \
  --item '{"PK":{"S":"ACCOUNT#acct-dest"},"SK":{"S":"METADATA"},"balance":{"N":"0"}}'
```

Send a signed test transaction:

```bash
python3 scripts/generate_hmac.py \
  --secret "$SIGNING_KEY" \
  --url "<your API address>/v1/webhooks/payment" \
  --amount 250.00 \
  --source-account acct-source \
  --destination-account acct-dest
```

This prints a ready-to-run `curl` command. Run it, and you should get back:

```json
{"status": "completed", "event_id": "..."}
```

A few things worth trying after that:
- Send the exact same request again → it recognizes the duplicate instead of
  charging twice.
- Try an amount bigger than the account's balance → it's rejected cleanly,
  no partial transaction.
- Tamper with the signature → the request is refused outright.

## Checking the results

**Live balances:**
```bash
aws dynamodb get-item --table-name FinancialLedger --region us-east-1 \
  --key '{"PK":{"S":"ACCOUNT#acct-source"},"SK":{"S":"METADATA"}}'
```

**Full transaction history:**
```bash
aws dynamodb query --table-name FinancialLedger --region us-east-1 \
  --index-name GSI1 \
  --key-condition-expression "GSI1PK = :status" \
  --expression-attribute-values '{":status":{"S":"STATUS#COMPLETED"}}'
```

**The permanent archive** (appears within about a minute of a transaction):
```bash
aws s3 ls s3://ledger-audit-archive-<your-account-id>-us-east-1/ --recursive
```

## Running the tests

```bash
pytest tests/unit -v
```

These run entirely locally against simulated AWS services, so no real
account or deployment is needed to verify the logic.

## Notes for anyone picking this up later

- The database and archive bucket are set to never be deleted automatically,
  even if the infrastructure is torn down. This is intentional, so a bad
  deploy can't wipe financial records by accident. Delete them by hand if
  you're cleaning up a throwaway test environment.
- The archive/report-generation piece relies on AWS's official pandas/Arrow
  layer rather than a custom-built one. Building it from scratch runs into
  Lambda's size limits for that combination of libraries.
- If you deploy somewhere other than `us-east-1`, a couple of ARNs in
  `stacks/ledger_stack.py` reference region-specific AWS resources and will
  need updating.
