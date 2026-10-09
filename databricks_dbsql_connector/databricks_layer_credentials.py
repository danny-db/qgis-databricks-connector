"""
Connection details for layers loaded from Databricks, without tokens in projects.

Layers keep the details needed to re-load them (Update Layer Data, Toggle Live
Mode) as custom properties, which QGIS writes into the .qgz. Those properties
never include an access token:

* the saved connection is referenced by name (``databricks/connection_name``)
  and its token is read from QGIS settings when needed;
* a token for a connection that hasn't been saved is held in memory for this
  QGIS session only;
* OAuth layers need no token at all (the cached sign-in is used).

Projects saved by older versions stored ``databricks/access_token`` on layers;
``migrate_layer`` removes it when such a project is opened, so the next save no
longer contains it.
"""
from qgis.PyQt.QtCore import QSettings
from qgis.core import Qgis, QgsMessageLog, QgsProject

from .databricks_auth import AUTH_PAT, normalise_auth_method

_GROUP = "DatabricksConnector/Connections"
_LEGACY_KEY_PROPERTY = "databricks/access_token"   # legacy; never written any more
_SESSION_TOKENS = {}                      # layer id -> token, memory only


def find_saved_connection(hostname, http_path, auth_method):
    """Name of a saved connection with these details, or ''."""
    settings = QSettings()
    settings.beginGroup(_GROUP)
    names = settings.childGroups()
    settings.endGroup()
    for name in names:
        base = f"{_GROUP}/{name}"
        if (settings.value(f"{base}/hostname", "") == hostname
                and settings.value(f"{base}/http_path", "") == http_path
                and normalise_auth_method(settings.value(f"{base}/auth_method", AUTH_PAT))
                == normalise_auth_method(auth_method)):
            return name
    return ""


def tag_layer(layer, connection_config):
    """Store connection details (no token) on a newly loaded layer."""
    hostname = connection_config.get("hostname", "")
    http_path = connection_config.get("http_path", "")
    auth_method = normalise_auth_method(connection_config.get("auth_method", AUTH_PAT))
    name = (connection_config.get("connection_name") or "").strip() \
        or find_saved_connection(hostname, http_path, auth_method)
    layer.setCustomProperty("databricks/hostname", hostname)
    layer.setCustomProperty("databricks/http_path", http_path)
    layer.setCustomProperty("databricks/auth_method", auth_method)
    layer.setCustomProperty("databricks/connection_name", name)
    layer.removeCustomProperty(_LEGACY_KEY_PROPERTY)
    token = connection_config.get("access_token", "")
    if auth_method == AUTH_PAT and token and not name:
        _SESSION_TOKENS[layer.id()] = token        # unsaved connection: this session only


def layer_connection_config(layer):
    """Connection details for re-loading a layer: saved connection first,
    then this session's token, then a legacy token property (old projects)."""
    config = {
        "hostname": layer.customProperty("databricks/hostname", ""),
        "http_path": layer.customProperty("databricks/http_path", ""),
        "access_token": "",  # nosec B105 - empty default; filled from the saved connection or session below
        "auth_method": normalise_auth_method(layer.customProperty("databricks/auth_method", AUTH_PAT)),
        "connection_name": layer.customProperty("databricks/connection_name", "") or "",
    }
    name = config["connection_name"]
    settings = QSettings()
    if name and settings.value(f"{_GROUP}/{name}/hostname", ""):
        base = f"{_GROUP}/{name}"
        config["hostname"] = settings.value(f"{base}/hostname", config["hostname"])
        config["http_path"] = settings.value(f"{base}/http_path", config["http_path"])
        config["auth_method"] = normalise_auth_method(settings.value(f"{base}/auth_method", config["auth_method"]))
        config["access_token"] = settings.value(f"{base}/access_token", "") or ""
    if not config["access_token"]:
        config["access_token"] = _SESSION_TOKENS.get(layer.id(), "") \
            or layer.customProperty(_LEGACY_KEY_PROPERTY, "") or ""
    return config


def credentials_ready(config):
    """True if the config can authenticate (a token, or OAuth)."""
    return bool(config["hostname"] and config["http_path"]
                and (config["access_token"] or config["auth_method"] != AUTH_PAT))


def migrate_layer(layer):
    """Remove a stored token from a layer saved by an older version.

    Links the layer to a matching saved connection when there is one;
    otherwise keeps the token in memory for this session. Returns True if a
    token was removed.
    """
    if not hasattr(layer, "customProperty"):
        return False
    token = layer.customProperty(_LEGACY_KEY_PROPERTY, "")
    if not token:
        return False
    hostname = layer.customProperty("databricks/hostname", "")
    http_path = layer.customProperty("databricks/http_path", "")
    name = layer.customProperty("databricks/connection_name", "") \
        or find_saved_connection(hostname, http_path, AUTH_PAT)
    if name:
        layer.setCustomProperty("databricks/connection_name", name)
    else:
        _SESSION_TOKENS[layer.id()] = token
    layer.removeCustomProperty(_LEGACY_KEY_PROPERTY)
    QgsMessageLog.logMessage(
        f"Upgraded layer '{layer.name()}' to the new connection format"
        + (f" (linked to saved connection '{name}')." if name else
           ". Save a connection for this workspace to re-load the layer in future sessions."),
        "Databricks Connector", Qgis.MessageLevel.Info)
    return True


def migrate_project():
    """Migrate every layer in the current project; returns how many changed."""
    return sum(migrate_layer(layer) for layer in QgsProject.instance().mapLayers().values())
