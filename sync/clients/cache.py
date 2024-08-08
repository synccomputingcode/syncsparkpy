import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Union, Callable, Type
from pathlib import Path
import json

from platformdirs import user_cache_dir

logger = logging.getLogger(__name__)


class CachedToken:
    def __init__(self, token_refresh_before_expiry=timedelta(seconds=30)):
        self.token_refresh_before_expiry = token_refresh_before_expiry

        cache = self._get_cached_token()

        if cache:
            self._access_token, self._access_token_expires_at_utc = cache
        else:
            self._access_token: Optional[str] = None
            self._access_token_expires_at_utc: Optional[datetime] = None

    @property
    def access_token(self) -> Optional[str]:
        return self._access_token if self.is_access_token_valid else None

    @property
    def is_access_token_valid(self) -> bool:
        if not self._access_token:
            return False

        if self._access_token_expires_at_utc:
            return datetime.now(tz=timezone.utc) < (
                    self._access_token_expires_at_utc - self.token_refresh_before_expiry
            )

        return False

    def set_cached_token(self, access_token: str, expires_at_utc: datetime) -> None:
        self._access_token = access_token
        self._access_token_expires_at_utc = expires_at_utc
        self._set_cached_token()

    def _set_cached_token(self) -> None:
        raise NotImplementedError

    def _get_cached_token(self) -> Optional[Tuple[str, datetime]]:
        raise NotImplementedError


class FileCachedToken(CachedToken):
    def __init__(self):
        self._cache_file = Path(user_cache_dir("syncsparkpy")) / "auth.json"

        super().__init__()

    def _get_cached_token(self) -> Optional[Tuple[str, datetime]]:
        # Cache is optional, we can fail to read it and not worry
        if self._cache_file.exists():
            try:
                cached_token = json.loads(self._cache_file.read_text())
                cached_access_token = cached_token["access_token"]
                cached_expiry = datetime.fromisoformat(cached_token["expires_at_utc"])
                return cached_access_token, cached_expiry
            except Exception as e:
                logger.warning(
                    f"Failed to read cached access token @ {self._cache_file}", exc_info=e
                )

        return None

    def _set_cached_token(self) -> None:
        # Cache is optional, we can fail to read it and not worry
        try:
            self._cache_file.parent.mkdir(parents=True, exist_ok=True)
            self._cache_file.write_text(
                json.dumps(
                    {
                        "access_token": self._access_token,
                        "expires_at_utc": self._access_token_expires_at_utc.isoformat(),
                    }
                )
            )
        except Exception as e:
            logger.warning(
                f"Failed to write cached access token @ {self._cache_file}", exc_info=e
            )


# Putting this here instead of config.py because circular imports and typing.
ACCESS_TOKEN_CACHE_CLS_TYPE = Union[Type[CachedToken], Callable[[], CachedToken]]
_access_token_cache_cls: ACCESS_TOKEN_CACHE_CLS_TYPE = FileCachedToken  # Default to local file caching.


def set_access_token_cache_cls(access_token_cache_cls: ACCESS_TOKEN_CACHE_CLS_TYPE) -> None:
    global _access_token_cache_cls
    _access_token_cache_cls = access_token_cache_cls


def get_access_token_cache_cache() -> ACCESS_TOKEN_CACHE_CLS_TYPE:
    global _access_token_cache_cls
    return _access_token_cache_cls
