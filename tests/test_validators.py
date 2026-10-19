import pytest

from src.validators import (
    validate_account,
    validate_amount,
    validate_currency,
    validate_timestamp,
    validate_transaction,
)


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


# =========================================================
# Amount validation
# =========================================================

def test_valid_amount_is_valid():
    assert validate_amount(100.0) is True


def test_zero_amount_is_invalid():
    assert validate_amount(0) is False


def test_negative_amount_is_invalid():
    assert validate_amount(-100.0) is False


def test_none_amount_is_invalid():
    assert validate_amount(None) is False


# =========================================================
# Account validation
# =========================================================

def test_valid_account_is_valid():
    assert validate_account("A001") is True


def test_none_account_is_invalid():
    assert validate_account(None) is False


def test_empty_account_is_invalid():
    assert validate_account("") is False


def test_whitespace_account_is_invalid():
    assert validate_account("   ") is False


# =========================================================
# Currency validation
# =========================================================

def test_valid_currency_is_valid():
    assert validate_currency(
        "US Dollar",
        ALLOWED_CURRENCIES,
    ) is True


def test_invalid_currency_is_invalid():
    assert validate_currency(
        "Mars Dollar",
        ALLOWED_CURRENCIES,
    ) is False


def test_none_currency_is_invalid():
    assert validate_currency(
        None,
        ALLOWED_CURRENCIES,
    ) is False


def test_empty_currency_is_invalid():
    assert validate_currency(
        "",
        ALLOWED_CURRENCIES,
    ) is False


def test_dataset_currency_is_valid():
    assert validate_currency(
        "Euro",
        ALLOWED_CURRENCIES,
    ) is True


# =========================================================
# Timestamp validation
# =========================================================

def test_valid_timestamp_is_valid():
    assert validate_timestamp(
        "2022/09/01 00:20",
        TIMESTAMP_FORMAT,
    ) is True


def test_invalid_timestamp_is_invalid():
    assert validate_timestamp(
        "2022-09-01 00:20:00",
        TIMESTAMP_FORMAT,
    ) is False


def test_none_timestamp_is_invalid():
    assert validate_timestamp(
        None,
        TIMESTAMP_FORMAT,
    ) is False


def test_empty_timestamp_is_invalid():
    assert validate_timestamp(
        "",
        TIMESTAMP_FORMAT,
    ) is False


# =========================================================
# Transaction validation
# =========================================================

def test_valid_transaction_returns_no_rejection_reasons():
    transaction = {
        "transaction_time": "2022/09/01 00:20",
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

    assert result["is_valid"] is True
    assert result["rejection_reasons"] == []


def test_invalid_transaction_returns_all_rejection_reasons():
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