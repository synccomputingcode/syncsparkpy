import datetime
from json import JSONEncoder
from typing import Any, Dict, TypeVar


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


KeyType = TypeVar("KeyType")


def deep_update(
    mapping: Dict[KeyType, Any], *updating_mappings: Dict[KeyType, Any]
) -> Dict[KeyType, Any]:
    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if k in updated_mapping:
                if isinstance(updated_mapping[k], dict) and isinstance(v, dict):
                    updated_mapping[k] = deep_update(updated_mapping[k], v)
                elif isinstance(updated_mapping[k], list) and isinstance(v, list):
                    updated_mapping[k] += v
                else:
                    updated_mapping[k] = v
            else:
                updated_mapping[k] = v
    return updated_mapping
