import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

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
