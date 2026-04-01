"""
Databricks SQL connection and query helpers for the API (no Streamlit).

- User authorization (OBO): pass the forwarded user access token from Databricks Apps
  (``X-Forwarded-Access-Token`` or ``Authorization: Bearer``). Before opening SQL, the token
  is exchanged at ``/oidc/v1/token`` for a Databricks ``sql``-scoped token unless
  ``GOVERNANCE_OBO_SQL_TOKEN_EXCHANGE`` is ``0`` (fixes common OpenSession 403 with IdP JWTs).
  Do not use ``DATABRICKS_CLIENT_ID`` for that exchange (Apps SP client lacks sql scope); optional
  ``DATABRICKS_SQL_TOKEN_EXCHANGE_CLIENT_ID`` only if your org assigns one. If exchange returns
  403 (sql not assigned to client id from the *user* JWT), we retry without ``scope=``; set
  ``GOVERNANCE_OBO_SQL_TOKEN_EXCHANGE_SCOPE=0`` to skip the scoped attempt. Connections are not
  cached and are closed per request (see ``main`` dependency).
- Local / legacy: omit the token and use unified SDK auth (PAT, profile, or app service
  principal from env) with a per-process cache keyed by warehouse HTTP path.
"""
from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
from typing import Any, Optional
from urllib.parse import urlencode

import pandas as pd

# In-process cache: (http_path, catalog, schema) -> connection (SP/PAT/profile only; not for OBO)
_connection_cache: dict[tuple[str, str, str], Any] = {}


def get_config():
    from databricks.sdk.core import Config
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "").strip()
    return Config(profile=profile) if profile else Config()


def _credentials_provider():
    cfg = get_config()
    def get_headers():
        return cfg.authenticate()
    return get_headers


def _normalize_server_host(raw: str) -> str:
    h = (raw or "").strip()
    h = h.replace("https://", "").replace("http://", "")
    h = h.strip("/").split("/")[0]
    h = h.split(":")[0]
    return h.strip()


def _server_hostname(*, host_override: Optional[str] = None) -> str:
    """Resolve workspace hostname. Prefer env / override over SDK Config (profile can point at wrong host)."""
    for candidate in (_normalize_server_host(host_override or ""), _normalize_server_host(os.environ.get("DATABRICKS_HOST", ""))):
        if candidate:
            return candidate
    cfg = get_config()
    host = _normalize_server_host(cfg.host or "")
    if not host:
        raise ValueError(
            "Databricks host not set. Set DATABRICKS_HOST (Databricks Apps sets this) or configure the SDK."
        )
    return host


def _exchange_obo_token_for_sql_access_token(server_host: str, subject_token: str) -> str:
    """
    Exchange the Databricks Apps user token (often an IdP JWT) for a Databricks OAuth access
    token via RFC 8693. OpenSession may 403 without this.

    The OAuth client id in errors (e.g. 80af788f-...) is taken from the *subject JWT* (Apps
    user-auth client), not from our form. If ``scope=sql`` is rejected for that client, we retry
    *without* ``scope`` so the server can return ``return_original_token_if_authenticated`` or
    delegate scopes from the subject token.
    """
    tls_no_verify = os.environ.get("DATABRICKS_TLS_NO_VERIFY", "").strip().lower() in ("1", "true", "yes")
    host_clean = _normalize_server_host(server_host)
    token_url = f"https://{host_clean}/oidc/v1/token"
    base: dict[str, str] = {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": subject_token,
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "return_original_token_if_authenticated": "true",
    }
    # Do NOT use DATABRICKS_CLIENT_ID here (Apps SP client); optional override only:
    client_id = (os.environ.get("DATABRICKS_SQL_TOKEN_EXCHANGE_CLIENT_ID") or "").strip()
    if client_id:
        base["client_id"] = client_id

    prefer_scope = (os.environ.get("GOVERNANCE_OBO_SQL_TOKEN_EXCHANGE_SCOPE") or "sql").strip()
    attempts: list[dict[str, str]] = []
    if prefer_scope and prefer_scope.lower() not in ("0", "none", "off"):
        attempts.append({**base, "scope": prefer_scope})
    attempts.append(dict(base))

    ctx = ssl.create_default_context()
    if tls_no_verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    last_failure: Optional[tuple[int, str]] = None
    for data in attempts:
        body = urlencode(data).encode("utf-8")
        req = urllib.request.Request(
            token_url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "*/*"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
                payload = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            err_body = e.read().decode(errors="replace") if e.fp else ""
            last_failure = (e.code, err_body)
            if (
                e.code == 403
                and "scope" in data
                and (
                    "not assigned to the client" in err_body
                    or "access_denied" in err_body
                )
            ):
                continue
            raise ValueError(
                f"SQL token exchange failed (HTTP {e.code}): {err_body or e.reason}. "
                f"If access_denied names an OAuth client id, it comes from the user token (Apps "
                f"user-auth client), not from DATABRICKS_CLIENT_ID in the request body. An admin "
                f"may need to register the sql scope for that OAuth app, or users must re-consent "
                f"after adding the sql scope on the app. Set GOVERNANCE_OBO_SQL_TOKEN_EXCHANGE_SCOPE=0 "
                f"to only attempt exchange without an explicit scope."
            ) from e
        except urllib.error.URLError as e:
            raise ValueError(f"SQL token exchange network error to {token_url}: {e}") from e
        at = payload.get("access_token")
        if not at:
            raise ValueError(f"SQL token exchange returned no access_token: {payload}")
        return str(at).strip()

    if last_failure:
        code, err_body = last_failure
        raise ValueError(
            f"SQL token exchange failed (HTTP {code}) after retry without scope: {err_body}. "
            f"Try GOVERNANCE_OBO_SQL_TOKEN_EXCHANGE=0 to use the forwarded token directly with "
            f"sql.connect, or ask an admin to allow sql on the Apps OAuth client shown in the error."
        )
    raise ValueError("SQL token exchange: no attempts made")


def format_sql_driver_error(exc: BaseException) -> str:
    """Best-effort detail from databricks-sql-connector errors (RequestError context, causes)."""
    msg = str(exc).strip() or type(exc).__name__
    mw = getattr(exc, "message_with_context", None)
    if callable(mw):
        try:
            full = str(mw()).strip()
            if full and full not in (msg, msg + ": {}"):
                return full
        except Exception:
            pass
    ctx = getattr(exc, "context", None)
    if isinstance(ctx, dict) and ctx:
        bits = []
        for k in ("http-code", "error-message", "method", "original-exception"):
            v = ctx.get(k)
            if v is not None and v != "":
                bits.append(f"{k}={v!r}")
        if bits:
            msg = f"{msg} ({', '.join(bits)})"
    if exc.__cause__ is not None:
        msg = f"{msg}; cause={exc.__cause__!r}"
    elif exc.__context__ is not None and exc.__context__ is not exc.__cause__:
        msg = f"{msg}; context={exc.__context__!r}"
    return msg


def get_connection(
    http_path: str,
    *,
    access_token: Optional[str] = None,
    host_override: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> Any:
    """Open a SQL warehouse connection.

    If ``access_token`` is set (Databricks Apps user / OBO token), it is used directly and
    the connection is not cached. If omitted, SDK unified auth is used and the connection
    is cached by ``http_path``.
    """
    from databricks import sql as databricks_sql
    tls_no_verify = os.environ.get("DATABRICKS_TLS_NO_VERIFY", "").strip().lower() in ("1", "true", "yes")
    host = _server_hostname(host_override=host_override)
    token = (access_token or "").strip()
    connect_kw: dict[str, Any] = {
        "server_hostname": host,
        "http_path": http_path,
        "_tls_no_verify": tls_no_verify,
    }
    if catalog:
        connect_kw["catalog"] = catalog
    if schema:
        connect_kw["schema"] = schema
    if token:
        # Same rule as token exchange: avoid Apps SP client_id (no sql scope on that OAuth client).
        fed_client = (os.environ.get("DATABRICKS_SQL_TOKEN_EXCHANGE_CLIENT_ID") or "").strip()
        if fed_client:
            connect_kw["identity_federation_client_id"] = fed_client
        # Apps OBO tokens are often IdP JWTs; SQL warehouse expects a Databricks token with sql scope.
        exchange_flag = (os.environ.get("GOVERNANCE_OBO_SQL_TOKEN_EXCHANGE") or "1").strip().lower()
        if exchange_flag not in ("0", "false", "no", "off"):
            try:
                token = _exchange_obo_token_for_sql_access_token(host, token)
            except ValueError:
                raise
            except Exception as ex:
                raise ValueError(
                    f"SQL token exchange failed unexpectedly: {ex}. "
                    f"Set GOVERNANCE_OBO_SQL_TOKEN_EXCHANGE=0 to skip pre-exchange and debug."
                ) from ex
        return databricks_sql.connect(access_token=token, **connect_kw)
    cache_key = (http_path, (catalog or "").strip(), (schema or "").strip())
    if cache_key in _connection_cache:
        return _connection_cache[cache_key]
    conn = databricks_sql.connect(
        credentials_provider=_credentials_provider,
        **connect_kw,
    )
    _connection_cache[cache_key] = conn
    return conn


def run_query(conn: Any, sql: str) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall_arrow()
        if result is None:
            return pd.DataFrame()
        return result.to_pandas()


def execute_statement(conn: Any, sql: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(sql)
    try:
        conn.commit()
    except Exception:
        pass


TABLE_CONFIG_WORKSPACES = "governance_config_workspaces"
TABLE_PREAPPROVED_IDENTITIES = "governance_preapproved_identities"
TABLE_VIOLATIONS_STAGING = "governance_violations_staging"
TABLE_CONTROL_ACTIONS = "governance_control_actions"
TABLE_FILTERS = "governance_filters"
TABLE_PENDING_APPROVALS = "governance_pending_approvals"
