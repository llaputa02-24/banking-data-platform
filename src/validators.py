def validate_amount(amount) -> bool:
    if amount is None:
        return False

    return amount > 0

def validate_account(account) -> bool:
    if account is None:
        return False

    if not isinstance(account, str):
        account = str(account)

    return account.strip() != ""

VALID_CURRENCIES = {
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

    # ISO-style codes
    "AUD",
    "BTC",
    "BRL",
    "CAD",
    "EUR",
    "MXN",
    "RUB",
    "INR",
    "SAR",
    "ILS",
    "CHF",
    "GBP",
    "USD",
    "JPY",
    "CNY",
}

def validate_currency(value, allowed_currencies):
    if value is None:
        return False

    value = value.strip()

    if not value:
        return False

    return value in allowed_currencies


from datetime import datetime


def validate_timestamp(value, timestamp_format):
    if value is None:
        return False

    try:
        datetime.strptime(
            value.strip(),
            timestamp_format
        )
        return True
    except (ValueError, TypeError):
        return False
    
def validate_transaction(
    transaction: dict,
    allowed_currencies,
    timestamp_format
) -> dict:

    rejection_reasons = []

    if not validate_timestamp(
        transaction.get("transaction_time"),
        timestamp_format
    ):
        rejection_reasons.append("INVALID_TIMESTAMP")

    if not validate_account(
        transaction.get("from_account")
    ):
        rejection_reasons.append("NULL_FROM_ACCOUNT")

    if not validate_account(
        transaction.get("to_account")
    ):
        rejection_reasons.append("NULL_TO_ACCOUNT")

    if not validate_amount(
        transaction.get("amount_paid")
    ):
        rejection_reasons.append("INVALID_AMOUNT")

    if not validate_currency(
        transaction.get("payment_currency"),
        allowed_currencies
    ):
        rejection_reasons.append("INVALID_PAYMENT_CURRENCY")

    return {
        "is_valid": len(rejection_reasons) == 0,
        "rejection_reasons": rejection_reasons,
    }