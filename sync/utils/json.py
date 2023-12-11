import datetime
from json import JSONEncoder


class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            date = obj
            if date.tzinfo is None:
                date = date.replace(tzinfo=datetime.timezone.utc)
            date = date.isoformat()
            date = date.replace("+00:00", "Z")
            return date


class DateTimeEncoderDropMicroseconds(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            date = obj
            date = date.replace(microsecond=0)
            if date.tzinfo is None:
                date = date.replace(tzinfo=datetime.timezone.utc)
            date = date.isoformat()
            date = date.replace("+00:00", "Z")
            return date
