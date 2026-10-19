from src.config_loader import load_config
from src.validators import validate_transaction


CONFIG_PATH = "config/transactions.yaml"


def route_transactions(transactions, config):
    """
    Simulate the Silver / Quarantine routing logic.

    Valid transactions -> silver_records
    Invalid transactions -> quarantine_records
    """

    allowed_currencies = set(
        config["validation"]["allowed_currencies"]
    )

    timestamp_format = config["validation"]["timestamp_format"]

    silver_records = []
    quarantine_records = []

    for transaction in transactions:
        result = validate_transaction(
            transaction,
            allowed_currencies,
            timestamp_format,
        )

        if result["is_valid"]:
            silver_records.append(transaction)

        else:
            quarantine_record = {
                **transaction,
                "rejection_reasons": result["rejection_reasons"],
            }

            quarantine_records.append(quarantine_record)

    return silver_records, quarantine_records


def test_transactions_are_routed_to_silver_and_quarantine():
    config = load_config(CONFIG_PATH)

    transactions = [
        # -------------------------
        # Valid transaction 1
        # -------------------------
        {
            "transaction_time": "2022/09/01 00:20",
            "from_account": "A001",
            "to_account": "B001",
            "amount_paid": 100.0,
            "payment_currency": "US Dollar",
        },

        # -------------------------
        # Valid transaction 2
        # -------------------------
        {
            "transaction_time": "2022/09/01 00:30",
            "from_account": "A002",
            "to_account": "B002",
            "amount_paid": 250.0,
            "payment_currency": "Euro",
        },

        # -------------------------
        # Invalid: missing account
        # -------------------------
        {
            "transaction_time": "2022/09/01 00:40",
            "from_account": None,
            "to_account": "B003",
            "amount_paid": 100.0,
            "payment_currency": "US Dollar",
        },

        # -------------------------
        # Invalid: negative amount
        # -------------------------
        {
            "transaction_time": "2022/09/01 00:50",
            "from_account": "A004",
            "to_account": "B004",
            "amount_paid": -100.0,
            "payment_currency": "US Dollar",
        },

        # -------------------------
        # Invalid: currency
        # -------------------------
        {
            "transaction_time": "2022/09/01 01:00",
            "from_account": "A005",
            "to_account": "B005",
            "amount_paid": 100.0,
            "payment_currency": "Mars Dollar",
        },

        # -------------------------
        # Invalid: timestamp
        # -------------------------
        {
            "transaction_time": "wrong-date",
            "from_account": "A006",
            "to_account": "B006",
            "amount_paid": 100.0,
            "payment_currency": "US Dollar",
        },

        # -------------------------
        # Invalid: multiple errors
        # -------------------------
        {
            "transaction_time": "wrong-date",
            "from_account": None,
            "to_account": "",
            "amount_paid": -500.0,
            "payment_currency": "Mars Dollar",
        },
    ]

    silver_records, quarantine_records = route_transactions(
        transactions,
        config,
    )

    # -------------------------
    # Reconciliation checks
    # -------------------------
    assert len(transactions) == 7

    assert len(silver_records) == 2

    assert len(quarantine_records) == 5

    assert (
        len(silver_records)
        + len(quarantine_records)
        == len(transactions)
    )

    # -------------------------
    # Check missing account
    # -------------------------
    assert (
        "NULL_FROM_ACCOUNT"
        in quarantine_records[0]["rejection_reasons"]
    )

    # -------------------------
    # Check negative amount
    # -------------------------
    assert (
        "INVALID_AMOUNT"
        in quarantine_records[1]["rejection_reasons"]
    )

    # -------------------------
    # Check invalid currency
    # -------------------------
    assert (
        "INVALID_PAYMENT_CURRENCY"
        in quarantine_records[2]["rejection_reasons"]
    )

    # -------------------------
    # Check invalid timestamp
    # -------------------------
    assert (
        "INVALID_TIMESTAMP"
        in quarantine_records[3]["rejection_reasons"]
    )

    # -------------------------
    # Check multiple errors
    # -------------------------
    multiple_error_record = quarantine_records[4]

    assert "INVALID_TIMESTAMP" in multiple_error_record[
        "rejection_reasons"
    ]

    assert "NULL_FROM_ACCOUNT" in multiple_error_record[
        "rejection_reasons"
    ]

    assert "NULL_TO_ACCOUNT" in multiple_error_record[
        "rejection_reasons"
    ]

    assert "INVALID_AMOUNT" in multiple_error_record[
        "rejection_reasons"
    ]

    assert "INVALID_PAYMENT_CURRENCY" in multiple_error_record[
        "rejection_reasons"
    ]