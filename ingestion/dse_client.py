"""Shared, resilient transport configuration for all bdshare ingestion jobs."""

from __future__ import annotations

import logging
import os
import threading
import time
import tempfile
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import certifi
import requests

logger = logging.getLogger(__name__)

DEFAULT_DSE_BASE_URL = "https://dsebd.org/"
_configured = False
_configure_lock = threading.Lock()


class DSEUpstreamError(RuntimeError):
    """Raised when the primary DSE host fails after bounded retries."""


def _combined_ca_bundle() -> str:
    """Add DSE's omitted issuing intermediate to the normal trusted roots."""
    root_bundle = Path(os.environ.get("DSE_CA_BUNDLE", certifi.where()))
    intermediate = (
        Path(__file__).resolve().parent
        / "certs"
        / "sectigo_public_server_authentication_ca_dv_r36.pem"
    )
    if not root_bundle.is_file():
        raise RuntimeError(f"DSE CA root bundle not found: {root_bundle}")
    if not intermediate.is_file():
        raise RuntimeError(f"DSE intermediate certificate not found: {intermediate}")

    destination = Path(tempfile.gettempdir()) / "aria-dse-ca-bundle.pem"
    destination.write_bytes(
        root_bundle.read_bytes().rstrip()
        + b"\n"
        + intermediate.read_bytes().lstrip()
    )
    return str(destination)


def _positive_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r", name, raw)
        return default
    if value <= 0:
        logger.warning("Ignoring non-positive %s=%r", name, raw)
        return default
    return value


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r", name, raw)
        return default
    if value <= 0:
        logger.warning("Ignoring non-positive %s=%r", name, raw)
        return default
    return value


def _base_url() -> str:
    base = os.environ.get("DSE_BASE_URL", DEFAULT_DSE_BASE_URL).strip()
    parsed = urlsplit(base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError(f"DSE_BASE_URL must be an absolute HTTP(S) URL, got {base!r}")
    return base.rstrip("/") + "/"


def _on_primary_host(url: str) -> str:
    """Keep the requested path/query but force the configured primary host."""
    requested = urlsplit(url)
    primary = urlsplit(_base_url())
    return urlunsplit(
        (
            primary.scheme,
            primary.netloc,
            requested.path or "/",
            requested.query,
            requested.fragment,
        )
    )


def _safe_get_primary(
    url: str,
    params: dict | None = None,
    alt_url: str | None = None,
    retries: int = 3,
    pause: float = 0.2,
    timeout: int | float = 10,
) -> requests.Response:
    """bdshare-compatible GET which never calls its invalid .com.bd fallback."""
    del alt_url, timeout  # Replaced with centrally bounded connect/read timeouts.

    from bdshare.util import helper

    target = _on_primary_host(url)
    attempts = _positive_int_env("DSE_RETRIES", retries)
    connect_timeout = _positive_float_env("DSE_CONNECT_TIMEOUT_SECONDS", 5.0)
    read_timeout = _positive_float_env("DSE_READ_TIMEOUT_SECONDS", 15.0)
    failures: list[str] = []

    for attempt in range(1, attempts + 1):
        if attempt > 1:
            time.sleep(pause * (2 ** (attempt - 2)))
        started = time.monotonic()
        try:
            response = helper._session.get(
                target,
                params=params,
                timeout=(connect_timeout, read_timeout),
            )
            elapsed = time.monotonic() - started
            if response.status_code == 200:
                logger.info("DSE GET %s succeeded in %.3fs", target, elapsed)
                return response
            detail = f"attempt {attempt}: HTTP {response.status_code} after {elapsed:.3f}s"
            failures.append(detail)
            logger.warning("DSE GET %s %s", target, detail)
        except requests.RequestException as exc:
            elapsed = time.monotonic() - started
            detail = f"attempt {attempt}: {type(exc).__name__} after {elapsed:.3f}s: {exc}"
            failures.append(detail)
            logger.warning("DSE GET %s %s", target, detail)

    raise DSEUpstreamError(
        f"DSE request failed after {attempts} attempts: {target}. "
        f"Failures: {'; '.join(failures)}"
    )


def get_bdshare_client():
    """Return bdshare after applying the shared primary-only transport once."""
    global _configured

    import bdshare  # type: ignore

    if _configured:
        return bdshare

    with _configure_lock:
        if _configured:
            return bdshare

        from bdshare.stock import market
        from bdshare.util import helper, vars as dse_vars

        base = _base_url()
        dse_vars.DSE_URL = base
        # Some bdshare functions implement fallback themselves. Pointing both
        # constants at the healthy host prevents all use of dsebd.com.bd.
        dse_vars.DSE_ALT_URL = base
        helper._session.verify = _combined_ca_bundle()
        helper.safe_get = _safe_get_primary
        # market.py imports safe_get into its module namespace.
        market.safe_get = _safe_get_primary

        _configured = True
        logger.info("Configured bdshare to use primary DSE host %s", base)

    return bdshare
