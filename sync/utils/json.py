import datetime
from json import JSONEncoder


class DefaultDateTimeEncoder(JSONEncoder):
    # this copies orjson's default behavior when serializing datetimes
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            date = obj
            date = date.isoformat()
            return date


class DateTimeEncoderNaiveUTC(JSONEncoder):
    # this copies orjson's behavior when used with the options OPT_UTC_Z and OPT_NAIVE_UTC
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            date = obj
            if date.tzinfo is None:
                date = date.replace(tzinfo=datetime.timezone.utc)
            date = date.isoformat()
            date = date.replace("+00:00", "Z")
            return date


class DateTimeEncoderNaiveUTCDropMicroseconds(JSONEncoder):
    # this copies orjson's behavior when used with the options OPT_OMIT_MICROSECONDS, OPT_UTC_Z, and OPT_NAIVE_UTC
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            date = obj
            date = date.replace(microsecond=0)
            if date.tzinfo is None:
                date = date.replace(tzinfo=datetime.timezone.utc)
            date = date.isoformat()
            date = date.replace("+00:00", "Z")
            return date
