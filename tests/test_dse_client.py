from __future__ import annotations

import requests

from ingestion.dse_client import DSEUpstreamError, _combined_ca_bundle, _safe_get_primary


class _Response:
    def __init__(self, status_code: int):
        self.status_code = status_code


class _Session:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.urls: list[str] = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        outcome = next(self.outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_combined_ca_bundle_includes_dse_intermediate():
    bundle = open(_combined_ca_bundle(), "rb").read()

    # The intermediate is PEM encoded, so assert its unique certificate prefix.
    assert b"MIIGTDCCBDSgAwIBAgIQOXpmzCdWNi4NqofKbqvjsT" in bundle


def test_safe_get_never_calls_invalid_fallback(monkeypatch):
    from bdshare.util import helper

    session = _Session([_Response(503), _Response(200)])
    monkeypatch.setattr(helper, "_session", session)
    monkeypatch.setenv("DSE_BASE_URL", "https://dsebd.org/")
    monkeypatch.setenv("DSE_RETRIES", "2")
    monkeypatch.setattr("ingestion.dse_client.time.sleep", lambda _: None)

    response = _safe_get_primary(
        "https://dsebd.org/latest_share_price_scroll_l.php",
        alt_url="https://dsebd.com.bd/latest_share_price_scroll_l.php",
    )

    assert response.status_code == 200
    assert session.urls == [
        "https://dsebd.org/latest_share_price_scroll_l.php",
        "https://dsebd.org/latest_share_price_scroll_l.php",
    ]


def test_safe_get_reports_every_attempt(monkeypatch):
    from bdshare.util import helper

    session = _Session([requests.Timeout("read timed out"), _Response(502)])
    monkeypatch.setattr(helper, "_session", session)
    monkeypatch.setenv("DSE_RETRIES", "2")
    monkeypatch.setattr("ingestion.dse_client.time.sleep", lambda _: None)

    try:
        _safe_get_primary("https://dsebd.org/dseX_share.php")
    except DSEUpstreamError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected DSEUpstreamError")

    assert "attempt 1: Timeout" in message
    assert "attempt 2: HTTP 502" in message
