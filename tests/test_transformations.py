from src.validators import validate_timestamp


TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"


def test_valid_timestamp_is_valid():
    assert validate_timestamp(
        "2022/09/01 00:20",
        TIMESTAMP_FORMAT,
    ) is True


def test_invalid_timestamp_is_invalid():
    assert validate_timestamp(
        "not-a-timestamp",
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