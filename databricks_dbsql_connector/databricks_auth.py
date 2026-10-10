"""Centralised authentication for the Databricks DBSQL Connector.

All Databricks connections in the plugin are built through this module so that
the choice of authentication method lives in exactly one place. Two methods are
supported:

* ``pat``       - Personal Access Token (the original behaviour).
* ``oauth_u2m`` - OAuth User-to-Machine (interactive browser login / SSO).

For OAuth U2M the underlying ``databricks-sql-connector`` opens a browser the
first time and exchanges the code for an access + refresh token. Because QGIS
opens many short-lived connections (test, discover, every layer load, every
live-layer viewport refresh, Genie), we persist those tokens to a
permission-restricted JSON file under the active QGIS profile directory and key
them by hostname. Subsequent connections read the cached refresh token and
renew silently, so the browser only appears once.

The bearer token cached by this flow is also reused for Genie's REST API calls,
which need an ``Authorization: Bearer`` header rather than a SQL connection.
"""

import base64
import json
import logging
import os
import re
import stat
import threading

# Authentication method identifiers (stored in QSettings and layer properties).
AUTH_PAT = "pat"
AUTH_OAUTH_U2M = "oauth_u2m"

# Human-readable labels for the UI combo box, in display order.
AUTH_METHOD_LABELS = [
    ("Personal Access Token", AUTH_PAT),
    ("OAuth (browser login / SSO)", AUTH_OAUTH_U2M),
]

# auth_type value understood by databricks-sql-connector for U2M OAuth.
_DATABRICKS_OAUTH_AUTH_TYPE = "databricks-oauth"

# The connector's default OAuth app ("databricks-sql-python") is only allowed
# the "sql" scope, so its tokens are rejected by the Genie REST API ("does not
# have required scopes: genie"). We sign in with the "databricks-cli" public
# client instead, which every workspace has and which may request "all-apis":
# one browser login then covers both SQL connections and Genie.
_OAUTH_CLIENT_ID = "databricks-cli"
_OAUTH_REDIRECT_PORT = 8020
_OAUTH_SCOPES = ["all-apis", "offline_access"]

# Refresh the bearer token this many seconds before it actually expires, so a
# multi-step operation (a Genie One conversation, a streamed model answer)
# never runs out of token part-way through.
_TOKEN_EXPIRY_MARGIN_SECONDS = 600

_persistence_lock = threading.Lock()

logger = logging.getLogger(__name__)


def normalise_auth_method(value):
    """Coerce a stored/blank auth-method value to a known identifier.

    Older saved connections and layers predate this field, so a missing or
    unrecognised value falls back to PAT to preserve existing behaviour.
    """
    if value == AUTH_OAUTH_U2M:
        return AUTH_OAUTH_U2M
    return AUTH_PAT


def _ensure_oauth_scopes():
    """Make the connector request ``_OAUTH_SCOPES`` during the browser login.

    databricks-sql-connector (3.5 - 4.x) has no keyword argument for OAuth
    scopes; it reads the module-level ``PYSQL_OAUTH_SCOPES`` list each time a
    connection is built, so we set it before connecting.
    """
    try:
        from databricks.sql.auth import auth as _dbsql_auth

        _dbsql_auth.PYSQL_OAUTH_SCOPES = list(_OAUTH_SCOPES)
    except Exception as exc:
        # SQL still works with the connector's default scopes; only Genie
        # needs the wider ones, and it reports a clear error if missing.
        logger.warning("Could not set OAuth scopes on the connector: %s", exc)


def _cache_key(hostname):
    """Return the hostname exactly as the connector keys persisted tokens.

    The connector normalises the host to ``https://<host>/`` before calling
    ``persist``/``read``, so lookups made outside the connector (Genie) must
    use the same form or they will never find the cached token.
    """
    host = (hostname or "").strip()
    if not host.startswith("https://"):
        host = "https://" + host
    if not host.endswith("/"):
        host += "/"
    return host


def _jwt_claims(token):
    """Decode the (unverified) claims of a JWT access token, or ``{}``."""
    try:
        payload_segment = token.split(".")[1]
        padding = "=" * (-len(payload_segment) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_segment + padding))
    except Exception:
        return {}


def _profile_oauth_token_path():
    """Return the path to the OAuth token cache inside the QGIS profile dir."""
    try:
        from qgis.core import QgsApplication

        base_dir = QgsApplication.qgisSettingsDirPath()
    except Exception:
        base_dir = os.path.join(os.path.expanduser("~"), ".qgis-databricks")
    if not base_dir:
        base_dir = os.path.join(os.path.expanduser("~"), ".qgis-databricks")
    return os.path.join(base_dir, "databricks_oauth_tokens.json")


def _restrict_permissions(path):
    """Best-effort lock down of the token cache to the current user (0600)."""
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass


def _make_oauth_persistence():
    """Build a QgisOAuthPersistence, or ``None`` if the connector is absent."""
    try:
        from databricks.sql.experimental.oauth_persistence import (
            OAuthPersistence,
            OAuthToken,
        )
    except Exception:
        return None

    class QgisOAuthPersistence(OAuthPersistence):
        """Persist OAuth tokens to a per-user JSON file keyed by hostname."""

        def __init__(self, path):
            self._path = path

        def _load_all(self):
            try:
                with open(self._path, "r", encoding="utf-8") as handle:
                    data = json.load(handle)
                    return data if isinstance(data, dict) else {}
            except (OSError, ValueError):
                return {}

        def persist(self, hostname, oauth_token):
            with _persistence_lock:
                data = self._load_all()
                data[hostname] = {
                    "access_token": oauth_token.access_token,
                    "refresh_token": oauth_token.refresh_token,
                }
                directory = os.path.dirname(self._path)
                if directory and not os.path.isdir(directory):
                    os.makedirs(directory, exist_ok=True)
                # Create the temp file 0600 from the start (no window where the
                # refresh token is world-readable), then swap it in atomically.
                tmp_path = self._path + ".tmp"
                fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                             stat.S_IRUSR | stat.S_IWUSR)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(data, handle)
                _restrict_permissions(tmp_path)
                os.replace(tmp_path, self._path)

        def read(self, hostname):
            with _persistence_lock:
                entry = self._load_all().get(hostname)
            if not entry:
                return None
            # Tokens cached before the switch to "all-apis" only carry the
            # "sql" scope and would fail Genie; ignore them so the user signs
            # in once more and gets a token that works everywhere.
            scope = _jwt_claims(entry.get("access_token") or "").get("scope", "")
            if "all-apis" not in scope.split():
                return None
            return OAuthToken(
                entry.get("access_token"), entry.get("refresh_token")
            )

    return QgisOAuthPersistence(_profile_oauth_token_path())


def connect_kwargs(hostname, http_path, access_token=None, auth_method=AUTH_PAT):
    """Build the keyword arguments for ``databricks.sql.connect``.

    The caller does ``sql.connect(**connect_kwargs(...))`` regardless of the
    auth method, so call sites no longer hard-code PAT.
    """
    method = normalise_auth_method(auth_method)
    kwargs = {"server_hostname": hostname, "http_path": http_path}
    if method == AUTH_OAUTH_U2M:
        # Lakehouse Real-Time warehouses only speak the kernel protocol, and the
        # kernel runs its own browser sign-in on every connection. Hand it the
        # plugin's cached OAuth token instead (renewed ahead of expiry), so one
        # sign-in keeps covering SQL, Genie and Explain this Map.
        # Uses the cache only; Sign in / Test Connection fill it (ensure_oauth_sign_in).
        token = get_bearer_token(hostname, None, auth_method=AUTH_OAUTH_U2M)
        if token and is_realtime_warehouse(hostname, http_path, token):
            kwargs.update(access_token=token, use_kernel=True)
            return kwargs
        _ensure_oauth_scopes()
        kwargs["auth_type"] = _DATABRICKS_OAUTH_AUTH_TYPE
        kwargs["oauth_client_id"] = _OAUTH_CLIENT_ID
        kwargs["oauth_redirect_port"] = _OAUTH_REDIRECT_PORT
        persistence = _make_oauth_persistence()
        if persistence is not None:
            kwargs["experimental_oauth_persistence"] = persistence
        return kwargs
    kwargs["access_token"] = access_token
    if access_token and is_realtime_warehouse(hostname, http_path, access_token):
        kwargs["use_kernel"] = True       # skip the classic protocol a Real-Time warehouse rejects
    return kwargs


def connect_kwargs_from_config(config):
    """Convenience wrapper for the dict shape used across the plugin."""
    return connect_kwargs(
        config.get("hostname"),
        config.get("http_path"),
        config.get("access_token"),
        config.get("auth_method", AUTH_PAT),
    )


def prime_oauth(hostname, http_path, auth_method=AUTH_OAUTH_U2M):
    """Run the interactive OAuth flow once and cache the tokens.

    Opens a single ``sql.connect`` so the browser login (and token persistence)
    happens up front, before background threads start opening their own
    connections and racing to launch competing browser windows. Returns a
    ``(success, message)`` tuple. Safe to call on a worker thread.
    """
    if normalise_auth_method(auth_method) != AUTH_OAUTH_U2M:
        return True, "No interactive sign-in required for this auth method."
    try:
        from databricks import sql
    except Exception:
        return False, "databricks-sql-connector is not installed."
    try:
        _interactive_oauth(hostname)      # browser once; tokens saved to the plugin's cache
        connection = sql.connect(
            **connect_kwargs(hostname, http_path, auth_method=AUTH_OAUTH_U2M)
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        connection.close()
        return True, "Signed in to Databricks successfully."
    except Exception as exc:  # noqa: BLE001 - surfaced to the user
        return False, "Sign-in failed: {}".format(exc)


def _jwt_seconds_until_expiry(token):
    """Return seconds until a JWT access token expires, or ``None`` if unknown.

    Reads the unsigned ``exp`` claim only; no signature verification is needed
    because we are merely deciding whether to refresh proactively.
    """
    try:
        payload_segment = token.split(".")[1]
        padding = "=" * (-len(payload_segment) % 4)
        decoded = base64.urlsafe_b64decode(payload_segment + padding)
        exp = json.loads(decoded).get("exp")
    except Exception:
        return None
    if not exp:
        return None
    try:
        import time

        return int(exp) - int(time.time())
    except Exception:
        return None


def _refresh_access_token(hostname, persistence, refresh_token):
    """Exchange the cached refresh token for a new access token now, via the
    workspace's OAuth token endpoint, and persist the new pair. Returns the
    access token, or None (the caller then falls back to a connector refresh)."""
    host = _clean_host(hostname)
    if not host:
        return None
    try:
        import urllib.parse
        import urllib.request
        from databricks.sql.experimental.oauth_persistence import OAuthToken

        body = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": _OAUTH_CLIENT_ID,
        }).encode("utf-8")
        request = urllib.request.Request(
            f"https://{host}/oidc/v1/token", data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
        with urllib.request.urlopen(request, timeout=30) as resp:  # nosec B310 - fixed https URL on the workspace host
            data = json.loads(resp.read().decode("utf-8"))
        access = data.get("access_token")
        if not access:
            return None
        persistence.persist(_cache_key(hostname),
                            OAuthToken(access, data.get("refresh_token") or refresh_token))
        return access
    except Exception as exc:
        logger.info("Early OAuth refresh failed, falling back to the connector: %s", exc)
        return None


def _clean_host(hostname):
    """Bare workspace host (no scheme, slash or path), or None if it looks unsafe."""
    host = (hostname or "").strip()
    host = host[len("https://"):] if host.startswith("https://") else host
    host = host.rstrip("/")
    if not host or any(c in host for c in "/@?#"):
        return None
    return host


_REALTIME_CACHE = {}


def is_realtime_warehouse(hostname, http_path, token):
    """True if ``http_path`` points at a Lakehouse Real-Time SQL warehouse.

    Asks the SQL Warehouses API once per warehouse (cached for the session).
    Any error means "not Real-Time", so standard warehouses are never affected.
    """
    match = re.search(r"/sql/1\.0/warehouses/([0-9A-Za-z]+)", http_path or "")
    host = _clean_host(hostname)
    if not match or not host or not token:
        return False
    key = (host, match.group(1))
    if key not in _REALTIME_CACHE:
        try:
            import urllib.request

            request = urllib.request.Request(
                f"https://{host}/api/2.0/sql/warehouses/{match.group(1)}",
                headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(request, timeout=20) as resp:  # nosec B310 - fixed https URL on the workspace host
                info = json.loads(resp.read().decode("utf-8"))
            _REALTIME_CACHE[key] = str(info.get("warehouse_type", "")).upper() == "REALTIME"
        except Exception as exc:
            logger.info("Could not read the warehouse type (assuming standard): %s", exc)
            _REALTIME_CACHE[key] = False
    return _REALTIME_CACHE[key]


def ensure_oauth_sign_in(hostname):
    """Make sure the plugin's OAuth cache holds a token (browser only if needed)."""
    return _interactive_oauth(hostname)


def _interactive_oauth(hostname):
    """Sign in through the browser (only if needed) and save the tokens to the
    plugin's cache, without opening a SQL session, so it works for every
    warehouse type. Returns the access token, or None."""
    persistence = _make_oauth_persistence()
    if persistence is None:
        return None
    _ensure_oauth_scopes()
    try:
        from databricks.sql.auth import auth as _dbsql_auth
        from databricks.sql.auth.authenticators import DatabricksOAuthProvider

        args = dict(hostname=_cache_key(hostname), oauth_persistence=persistence,
                    redirect_port_range=[_OAUTH_REDIRECT_PORT], client_id=_OAUTH_CLIENT_ID,
                    scopes=list(_dbsql_auth.PYSQL_OAUTH_SCOPES))
        try:   # connector 4.x needs an HTTP client; 3.x does not take one
            from databricks.sql import __version__ as _dbsql_version
            from databricks.sql.common.unified_http_client import UnifiedHttpClient
            from databricks.sql.utils import build_client_context

            client = UnifiedHttpClient(build_client_context(hostname, _dbsql_version))
            provider = DatabricksOAuthProvider(http_client=client, **args)
        except ImportError:
            provider = DatabricksOAuthProvider(**args)
        headers = {}
        provider.add_headers(headers)
        auth = headers.get("Authorization", "")
        return auth[len("Bearer "):] if auth.startswith("Bearer ") else None
    except Exception as exc:
        logger.warning("OAuth sign-in failed: %s", exc)
        return None


def get_bearer_token(hostname, http_path, access_token=None, auth_method=AUTH_PAT):
    """Return a bearer token for Databricks REST calls (used by Genie).

    For PAT this is just the token. For OAuth U2M it returns the cached access
    token, refreshing it through a lightweight ``sql.connect`` ping when the
    cached token is missing or about to expire.
    """
    method = normalise_auth_method(auth_method)
    if method != AUTH_OAUTH_U2M:
        return access_token

    persistence = _make_oauth_persistence()
    cached = persistence.read(_cache_key(hostname)) if persistence is not None else None
    token = cached.access_token if cached is not None else None

    seconds_left = _jwt_seconds_until_expiry(token) if token else None
    needs_refresh = (
        token is None
        or (seconds_left is not None and seconds_left < _TOKEN_EXPIRY_MARGIN_SECONDS)
    )
    if needs_refresh and cached is not None and cached.refresh_token and persistence is not None:
        # Refresh ahead of expiry: the connector itself only refreshes tokens
        # that have already expired.
        fresh = _refresh_access_token(hostname, persistence, cached.refresh_token)
        if fresh:
            return fresh
    if needs_refresh and http_path:
        # A real connection forces the connector to refresh and re-persist.
        try:
            from databricks import sql

            connection = sql.connect(
                **connect_kwargs(hostname, http_path, auth_method=AUTH_OAUTH_U2M)
            )
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            connection.close()
            cached = persistence.read(_cache_key(hostname)) if persistence is not None else None
            token = cached.access_token if cached is not None else token
        except Exception as exc:
            # Fall back to whatever we had cached; the caller surfaces errors.
            logger.warning("OAuth token refresh failed, using cached token: %s", exc)
    return token
