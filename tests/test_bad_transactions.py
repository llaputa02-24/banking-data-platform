from src.validators import validate_transaction

from src.validators import validate_transaction


ALLOWED_CURRENCIES = {
    "Australian Dollar",
    "Bitcoin",
    "Brazil Real",
    "Canadian Dollar",
    "Euro",
    "Mexican Peso",
    "Ruble",
    "Rupee",
    "Saudi Riyal",
    "Shekel",
    "Swiss Franc",
    "UK Pound",
    "US Dollar",
    "Yen",
    "Yuan",
}

TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"


def test_missing_from_account_is_rejected():
    transaction = {
        "transaction_time": "2022/09/01 00:20",
        "from_account": None,
        "to_account": "8000EBD30",
        "amount_paid": 100.0,
        "payment_currency": "US Dollar",
    }

    result = validate_transaction(
        transaction,
        ALLOWED_CURRENCIES,
        TIMESTAMP_FORMAT,
    )

    assert result["is_valid"] is False
    assert "NULL_FROM_ACCOUNT" in result["rejection_reasons"]


def test_negative_amount_is_rejected():
    transaction = {
        "transaction_time": "2022/09/01 00:20",
        "from_account": "A001",
        "to_account": "B001",
        "amount_paid": -100.0,
        "payment_currency": "US Dollar",
    }

    result = validate_transaction(
        transaction,
        ALLOWED_CURRENCIES,
        TIMESTAMP_FORMAT,
    )

    assert result["is_valid"] is False
    assert "INVALID_AMOUNT" in result["rejection_reasons"]


def test_invalid_currency_is_rejected():
    transaction = {
        "transaction_time": "2022/09/01 00:20",
        "from_account": "A001",
        "to_account": "B001",
        "amount_paid": 100.0,
        "payment_currency": "Mars Dollar",
    }

    result = validate_transaction(
        transaction,
        ALLOWED_CURRENCIES,
        TIMESTAMP_FORMAT,
    )

    assert result["is_valid"] is False
    assert "INVALID_PAYMENT_CURRENCY" in result["rejection_reasons"]


def test_invalid_timestamp_is_rejected():
    transaction = {
        "transaction_time": "wrong-date",
        "from_account": "A001",
        "to_account": "B001",
        "amount_paid": 100.0,
        "payment_currency": "US Dollar",
    }

    result = validate_transaction(
        transaction,
        ALLOWED_CURRENCIES,
        TIMESTAMP_FORMAT,
    )

    assert result["is_valid"] is False
    assert "INVALID_TIMESTAMP" in result["rejection_reasons"]


def test_multiple_errors_are_all_recorded():
    transaction = {
        "transaction_time": "wrong-date",
        "from_account": None,
        "to_account": "",
        "amount_paid": -100.0,
        "payment_currency": "Mars Dollar",
    }

    result = validate_transaction(
        transaction,
        ALLOWED_CURRENCIES,
        TIMESTAMP_FORMAT,
    )

    assert result["is_valid"] is False

    assert "INVALID_TIMESTAMP" in result["rejection_reasons"]
    assert "NULL_FROM_ACCOUNT" in result["rejection_reasons"]
    assert "NULL_TO_ACCOUNT" in result["rejection_reasons"]
    assert "INVALID_AMOUNT" in result["rejection_reasons"]
    assert "INVALID_PAYMENT_CURRENCY" in result["rejection_reasons"]