from datetime import UTC
from datetime import datetime as dt


def utc_now():
    return dt.now(UTC)
