import datetime
import sys


def utcnow():
    if sys.version_info >= (3, 12):
        from datetime import UTC

        return datetime.datetime.now(UTC)
    else:
        return datetime.datetime.utcnow()
