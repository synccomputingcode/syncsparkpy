import orjson

from sync import __version__

USER_AGENT = f"Sync Library/{__version__} (syncsparkpy)"


def encode_json(obj: dict) -> tuple[dict, str]:
    # "%Y-%m-%dT%H:%M:%SZ"
    options = orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_NAIVE_UTC

    json = orjson.dumps(obj, option=options).decode()

    return {
        "Content-Length": str(len(json)),
        "Content-Type": "application/json",
    }, json
