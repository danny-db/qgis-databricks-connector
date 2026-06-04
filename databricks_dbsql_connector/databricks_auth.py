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
import os
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

# Refresh the bearer token this many seconds before it actually expires, so a
# request never goes out with a token that lapses mid-flight.
_TOKEN_EXPIRY_MARGIN_SECONDS = 120

_persistence_lock = threading.Lock()


def normalise_auth_method(value):
    """Coerce a stored/blank auth-method value to a known identifier.

    Older saved connections and layers predate this field, so a missing or
    unrecognised value falls back to PAT to preserve existing behaviour.
    """
    if value == AUTH_OAUTH_U2M:
        return AUTH_OAUTH_U2M
    return AUTH_PAT


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
                with open(self._path, "w", encoding="utf-8") as handle:
                    json.dump(data, handle)
                _restrict_permissions(self._path)

        def read(self, hostname):
            with _persistence_lock:
                entry = self._load_all().get(hostname)
            if not entry:
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
        kwargs["auth_type"] = _DATABRICKS_OAUTH_AUTH_TYPE
        persistence = _make_oauth_persistence()
        if persistence is not None:
            kwargs["experimental_oauth_persistence"] = persistence
        return kwargs
    kwargs["access_token"] = access_token
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
    cached = persistence.read(hostname) if persistence is not None else None
    token = cached.access_token if cached is not None else None

    seconds_left = _jwt_seconds_until_expiry(token) if token else None
    needs_refresh = (
        token is None
        or (seconds_left is not None and seconds_left < _TOKEN_EXPIRY_MARGIN_SECONDS)
    )
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
            cached = persistence.read(hostname) if persistence is not None else None
            token = cached.access_token if cached is not None else token
        except Exception:
            # Fall back to whatever we had cached; the caller surfaces errors.
            pass
    return token
