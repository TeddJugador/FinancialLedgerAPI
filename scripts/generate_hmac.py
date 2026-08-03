#!/usr/bin/env python3
"""
Generates a valid X-Signature-256 header for a sample webhook payload so you
can manually curl the deployed /v1/webhooks/payment endpoint.

Usage:
    python3 scripts/generate_hmac.py --secret "<signing_key>" [--amount 250.00]

Then copy the printed curl command, or pipe it straight to a shell:
    python3 scripts/generate_hmac.py --secret "$(aws secretsmanager get-secret-value \\
        --secret-id ledger/webhook-signing-secret \\
        --query 'SecretString' --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)["signing_key"])')" \\
      | bash
"""
import argparse
import hashlib
import hmac
import json
import uuid


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secret", required=True, help="Webhook HMAC signing key")
    parser.add_argument("--url", default="https://<api-id>.execute-api.<region>.amazonaws.com/v1/webhooks/payment")
    parser.add_argument("--event-id", default=str(uuid.uuid4()))
    parser.add_argument("--amount", default="250.00")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--source-account", default="acct-source-001")
    parser.add_argument("--destination-account", default="acct-dest-002")
    args = parser.parse_args()

    payload = {
        "event_id": args.event_id,
        "amount": args.amount,
        "currency": args.currency,
        "source_account": args.source_account,
        "destination_account": args.destination_account,
    }
    # NOTE: json.dumps output must match byte-for-byte what curl sends as
    # --data-raw below (no extra whitespace), since the Lambda verifies the
    # signature over the exact raw request body.
    body = json.dumps(payload, separators=(",", ":"))

    signature = hmac.new(
        key=args.secret.encode("utf-8"),
        msg=body.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    print("# Payload:")
    print(body)
    print()
    print("# Signature (X-Signature-256):")
    print(signature)
    print()
    print("# Ready-to-run curl command:")
    print(
        f"curl -i -X POST '{args.url}' \\\n"
        f"  -H 'Content-Type: application/json' \\\n"
        f"  -H 'X-Signature-256: {signature}' \\\n"
        f"  --data-raw '{body}'"
    )


if __name__ == "__main__":
    main()
