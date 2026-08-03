"""HMAC-SHA256 webhook signature verification helpers."""
import hashlib
import hmac


def compute_signature(raw_body: bytes, secret: str) -> str:
    """Compute the hex-encoded HMAC-SHA256 signature for a raw request body."""
    return hmac.new(
        key=secret.encode("utf-8"),
        msg=raw_body,
        digestmod=hashlib.sha256,
    ).hexdigest()


def is_valid_signature(raw_body: bytes, secret: str, provided_signature: str) -> bool:
    """
    Constant-time comparison of a provided signature against the expected one.

    Supports signatures optionally prefixed with 'sha256=' (Stripe/GitHub style)
    by stripping the scheme before comparing.
    """
    if not provided_signature:
        return False

    candidate = provided_signature.strip()
    if candidate.startswith("sha256="):
        candidate = candidate[len("sha256="):]

    expected = compute_signature(raw_body, secret)
    return hmac.compare_digest(expected, candidate)
