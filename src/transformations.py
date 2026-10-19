import hashlib



TRANSACTION_ID_FIELDS = [
    "transaction_time",
    "from_account",
    "to_account",
    "amount_paid",
    "payment_currency",
    "payment_format",
]


def generate_transaction_id(transaction: dict) -> str:
    for field in TRANSACTION_ID_FIELDS:
        if field not in transaction:
            raise ValueError(f"Missing required field: {field}")

    raw_value = "|".join(
        str(transaction[field])
        for field in TRANSACTION_ID_FIELDS
    )

    return hashlib.sha256(
        raw_value.encode("utf-8")
    ).hexdigest()