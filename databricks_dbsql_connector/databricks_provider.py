"""
Databricks vector data provider ("databricks").

Layers backed by this provider are saved in the QGIS project as a URI and
re-query Databricks whenever the project is opened, like PostGIS layers. The
source is either a Unity Catalog table or a custom SQL query (issue #3):

    databricks://<host>:443<http_path>?table=cat.sch.tbl&geom_column=geom&conn=<saved connection>
    databricks://<host>:443<http_path>?sql=<url-encoded SELECT>&geom_column=geom&auth_method=oauth_u2m

Credentials are never written into the URI (and so never into the .qgz):
``conn`` names a saved connection whose host, path, auth method and token are
read from QGIS settings when the layer loads. OAuth layers can also work from
host + path alone, using the cached sign-in. Older URIs carrying
``access_token`` still load, for backwards compatibility.

QGIS asks for features per map extent; each request becomes one query with the
extent bound as a parameter (``ST_INTERSECTS(geom, ST_GEOMFROMTEXT(:wkt, :srid))``).
Feature ids are stable across requests so selection and identify work.
"""
import hashlib
import re
import threading
import urllib.parse
from collections import OrderedDict

from qgis.PyQt.QtCore import QSettings, QVariant
from qgis.core import (
    QgsVectorDataProvider,
    QgsAbstractFeatureSource,
    QgsAbstractFeatureIterator,
    QgsFeatureIterator,
    QgsFeatureRequest,
    QgsFeature,
    QgsFields,
    QgsField,
    QgsGeometry,
    QgsWkbTypes,
    QgsRectangle,
    QgsCoordinateReferenceSystem,
    QgsProviderMetadata,
    QgsDataProvider,
    QgsMessageLog,
    Qgis,
)
from databricks import sql
from .databricks_auth import AUTH_PAT, AUTH_OAUTH_U2M, normalise_auth_method, connect_kwargs

_LOG_TAG = "Databricks Provider"
_FID_CACHE_SIZE = 200000          # features remembered for select/identify by id
_CONNECTIONS_GROUP = "DatabricksConnector/Connections"


def _log(message, level=Qgis.MessageLevel.Info):
    QgsMessageLog.logMessage(message, _LOG_TAG, level)


def quote_identifier(name):
    """Backtick-quote a Databricks identifier, doubling inner backticks."""
    return "`" + str(name).strip("`").replace("`", "``") + "`"


def clean_sql(query):
    """Normalise a user query for use as a sub-select: trim and drop
    trailing semicolons. Only a single SELECT / WITH statement is allowed."""
    text = (query or "").strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    body = re.sub(r"^(\s*(--[^\n]*\n|/\*.*?\*/))*", "", text, flags=re.S).lstrip()
    if not re.match(r"(?is)^(select|with)\b", body):
        raise ValueError("Only a single SELECT (or WITH ... SELECT) query can be saved as a layer.")
    if ";" in re.sub(r"'(?:[^']|'')*'", "", text):
        raise ValueError("Only one SQL statement can be saved as a layer.")
    return text


def build_uri(connection_config, table=None, sql_query=None, geom_column=None):
    """Build a 'databricks' provider URI for a table or a SQL query.

    Never includes a token: a saved connection is referenced by name, and
    OAuth layers fall back to host + path + the cached sign-in. A PAT
    connection must be saved first so its token can be looked up locally.
    """
    hostname = connection_config.get("hostname", "")
    http_path = connection_config.get("http_path", "")
    auth_method = normalise_auth_method(connection_config.get("auth_method", AUTH_PAT))
    conn_name = (connection_config.get("connection_name") or "").strip()
    if auth_method == AUTH_PAT and not conn_name:
        raise ValueError("Save this connection first: layers saved in a project reference the "
                         "saved connection by name, so the access token never goes into the project file.")
    params = []
    if table:
        params.append(("table", table))
    if sql_query:
        params.append(("sql", clean_sql(sql_query)))
    if geom_column:
        params.append(("geom_column", geom_column))
    if conn_name:
        params.append(("conn", conn_name))
    params.append(("auth_method", auth_method))
    return f"databricks://{hostname}:443{http_path}?{urllib.parse.urlencode(params)}"


def _qgs_type(databricks_type):
    """Map a Databricks type (e.g. 'decimal(10,2)', 'geometry(0)') to a QVariant type."""
    base = databricks_type.split("(")[0].split("<")[0].strip().upper()
    return {
        "STRING": QVariant.String, "VARCHAR": QVariant.String, "CHAR": QVariant.String,
        "INT": QVariant.Int, "INTEGER": QVariant.Int, "SMALLINT": QVariant.Int, "TINYINT": QVariant.Int,
        "BIGINT": QVariant.LongLong, "LONG": QVariant.LongLong,
        "FLOAT": QVariant.Double, "DOUBLE": QVariant.Double, "DECIMAL": QVariant.Double,
        "BOOLEAN": QVariant.Bool, "DATE": QVariant.Date,
        "TIMESTAMP": QVariant.DateTime, "TIMESTAMP_NTZ": QVariant.DateTime,
    }.get(base, QVariant.String)


def _is_geo_type(databricks_type):
    return databricks_type.strip().upper().startswith(("GEOMETRY", "GEOGRAPHY"))


def _wkb_type(type_names):
    """Pick one QGIS geometry type from Databricks ST_GEOMETRYTYPE names."""
    kinds = set()
    for name in type_names:
        n = (name or "").upper().replace("ST_", "")
        for kind in ("MULTIPOLYGON", "MULTILINESTRING", "MULTIPOINT", "POLYGON", "LINESTRING", "POINT"):
            if kind in n:       # multi* checked first: 'MULTIPOINT' contains 'POINT'
                kinds.add(kind)
                break
        else:
            kinds.add("OTHER")
    single = {"POINT": QgsWkbTypes.Type.Point, "LINESTRING": QgsWkbTypes.Type.LineString,
              "POLYGON": QgsWkbTypes.Type.Polygon}
    multi = {"MULTIPOINT": QgsWkbTypes.Type.MultiPoint, "MULTILINESTRING": QgsWkbTypes.Type.MultiLineString,
             "MULTIPOLYGON": QgsWkbTypes.Type.MultiPolygon}
    if len(kinds) == 1:
        kind = next(iter(kinds))
        return single.get(kind) or multi.get(kind) or QgsWkbTypes.Type.Unknown
    # e.g. Polygon + MultiPolygon -> MultiPolygon (QGIS stores single parts in multi layers)
    bases = {k.replace("MULTI", "") for k in kinds}
    if len(bases) == 1 and "OTHER" not in kinds:
        return multi["MULTI" + next(iter(bases))]
    return QgsWkbTypes.Type.Unknown


class DatabricksFeatureIterator(QgsAbstractFeatureIterator):
    """Iterates the rows one feature request returns."""

    def __init__(self, source, request):
        super().__init__(request)
        self._source = source
        self._rows = source.provider.fetch_rows(request)
        self._index = 0

    def fetchFeature(self, f):
        if self._index >= len(self._rows):
            return False
        fid, attrs, wkb = self._rows[self._index]
        self._index += 1
        f.setFields(self._source.provider.fields_cache)
        f.setId(fid)
        f.setAttributes(attrs)
        if wkb:
            geom = QgsGeometry()
            geom.fromWkb(bytes(wkb))
            f.setGeometry(geom)
        else:
            f.setGeometry(QgsGeometry())
        f.setValid(True)
        return True

    def rewind(self):
        self._index = 0
        return True

    def close(self):
        self._rows = []
        return True


class DatabricksFeatureSource(QgsAbstractFeatureSource):
    """Feature source handed to QGIS render threads."""

    def __init__(self, provider):
        super().__init__()
        self.provider = provider

    def getFeatures(self, request=QgsFeatureRequest()):
        return QgsFeatureIterator(DatabricksFeatureIterator(self, request))


class DatabricksProvider(QgsVectorDataProvider):
    """Read-only vector provider for a Databricks table or SQL query."""

    PROVIDER_KEY = 'databricks'
    PROVIDER_DESCRIPTION = 'Databricks SQL Data Provider'

    def __init__(self, uri='', options=None, flags=None):
        super().__init__(uri, options or QgsDataProvider.ProviderOptions(),
                         flags or Qgis.DataProviderReadFlags())
        self.uri_string = uri
        self.connection = None
        self._lock = threading.RLock()       # QGIS renders from worker threads
        self.fields_cache = QgsFields()
        self.feature_count_cache = -1
        self.extent_cache = QgsRectangle()
        self.geometry_column = None
        self.geometry_type = QgsWkbTypes.Type.Unknown
        self.data_srid = 0                   # SRID stored in Databricks
        self.layer_crs = QgsCoordinateReferenceSystem("EPSG:4326")
        self._fid_cache = OrderedDict()      # fid -> (attrs, wkb)
        self.error_message = ""

        self._parse_uri(uri)
        if self.is_valid_config():
            self._connect()
            if self.connection is not None:
                self._initialize_layer()

    # -- URI + credentials ------------------------------------------------

    def _parse_uri(self, uri):
        self.hostname = ''
        self.http_path = ''
        self.access_token = ''  # nosec B105 - empty default; resolved from the saved connection
        self.auth_method = AUTH_PAT
        self.table_name = ''
        self.sql_query = ''
        self.connection_name = ''
        self.requested_geom = ''
        self.requested_srid = None
        if not uri.startswith('databricks://'):
            return
        try:
            parsed = urllib.parse.urlparse(uri)
            self.hostname = parsed.hostname or ''
            self.http_path = parsed.path or ''
            params = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query, keep_blank_values=True).items()}
            self.table_name = params.get('table', '')
            self.sql_query = params.get('sql', '')
            self.requested_geom = params.get('geom_column', '')
            self.connection_name = params.get('conn', '')
            self.auth_method = normalise_auth_method(params.get('auth_method', AUTH_PAT))
            self.access_token = params.get('access_token', '')     # legacy URIs only
            if params.get('srid', '').isdigit():
                self.requested_srid = int(params['srid'])
        except Exception as exc:
            _log(f"Error parsing URI: {exc}", Qgis.MessageLevel.Warning)
            return
        if self.connection_name:
            self._load_saved_connection()

    def _load_saved_connection(self):
        """Resolve host/path/auth/token from the saved connection named in the URI."""
        settings = QSettings()
        base = f"{_CONNECTIONS_GROUP}/{self.connection_name}"
        if not settings.value(f"{base}/hostname", ""):
            self.error_message = (f"Saved Databricks connection '{self.connection_name}' was not found. "
                                  "Create it in the Databricks dialog with the same name.")
            _log(self.error_message, Qgis.MessageLevel.Warning)
            return
        self.hostname = settings.value(f"{base}/hostname", self.hostname)
        self.http_path = settings.value(f"{base}/http_path", self.http_path)
        self.auth_method = normalise_auth_method(settings.value(f"{base}/auth_method", self.auth_method))
        self.access_token = settings.value(f"{base}/access_token", "") or self.access_token

    def is_valid_config(self):
        token_ok = bool(self.access_token) or self.auth_method == AUTH_OAUTH_U2M
        return bool(self.hostname and self.http_path and token_ok and (self.table_name or self.sql_query))

    def _connect(self):
        try:
            self.connection = sql.connect(**connect_kwargs(
                self.hostname, self.http_path, self.access_token, self.auth_method))
        except Exception as exc:
            self.error_message = f"Failed to connect to Databricks: {exc}"
            _log(self.error_message, Qgis.MessageLevel.Critical)
            self.connection = None

    # -- SQL helpers --------------------------------------------------------

    def source_sql(self):
        """FROM-clause source: a quoted table or the user's query as a sub-select."""
        if self.sql_query:
            return f"({clean_sql(self.sql_query)}) AS `__dbx_layer_src`"
        return ".".join(quote_identifier(p) for p in self.table_name.split("."))

    def _query(self, statement, params=None):
        """Run a statement under the provider lock; reconnect once if the
        connection dropped (e.g. a project left open overnight)."""
        with self._lock:
            for attempt in (1, 2):
                try:
                    with self.connection.cursor() as cursor:
                        cursor.execute(statement, params) if params else cursor.execute(statement)
                        return cursor.fetchall()
                except Exception:
                    if attempt == 2:
                        raise
                    _log("Databricks query failed; reconnecting once", Qgis.MessageLevel.Info)
                    self._connect()
                    if self.connection is None:
                        raise

    def _initialize_layer(self):
        src = self.source_sql()
        try:
            schema = self._query(f"DESCRIBE QUERY SELECT * FROM {src}")  # nosec B608 - source is a backtick-quoted table or the user's own SELECT
        except Exception as exc:
            self.error_message = f"Could not read the layer's columns: {exc}"
            _log(self.error_message, Qgis.MessageLevel.Critical)
            self.connection = None
            return

        geo_columns = []
        self.fields_cache = QgsFields()
        for row in schema:
            name, dtype = row[0], str(row[1])
            if not name or name.startswith("#"):
                continue
            if _is_geo_type(dtype):
                geo_columns.append(name)
            else:
                self.fields_cache.append(QgsField(name, _qgs_type(dtype)))
        # Honour the requested geometry column; otherwise the first geometry column
        wanted = [c for c in geo_columns if c.lower() == self.requested_geom.lower()] if self.requested_geom else []
        self.geometry_column = (wanted or geo_columns or [None])[0]
        for extra in geo_columns:      # other geometry columns become text attributes
            if extra != self.geometry_column:
                self.fields_cache.append(QgsField(extra, QVariant.String))

        try:
            if self.geometry_column:
                g = quote_identifier(self.geometry_column)
                stats = self._query(
                    f"SELECT COUNT(*), MIN(ST_XMIN({g})), MIN(ST_YMIN({g})), MAX(ST_XMAX({g})), "
                    f"MAX(ST_YMAX({g})), MIN(ST_SRID({g})) FROM {src}")[0]  # nosec B608 - identifiers backtick-quoted; source is a quoted table or the user's own SELECT
                self.feature_count_cache = int(stats[0] or 0)
                if all(v is not None for v in stats[1:5]):
                    self.extent_cache = QgsRectangle(stats[1], stats[2], stats[3], stats[4])
                self.data_srid = int(stats[5] or 0)
                types = self._query(
                    f"SELECT DISTINCT ST_GEOMETRYTYPE({g}) FROM {src} WHERE {g} IS NOT NULL LIMIT 10")  # nosec B608 - identifiers backtick-quoted; source is a quoted table or the user's own SELECT
                self.geometry_type = _wkb_type([r[0] for r in types])
            else:
                self.feature_count_cache = int(self._query(f"SELECT COUNT(*) FROM {src}")[0][0] or 0)  # nosec B608 - source is a quoted table or the user's own SELECT
                self.geometry_type = QgsWkbTypes.Type.NoGeometry
        except Exception as exc:
            _log(f"Could not read layer statistics: {exc}", Qgis.MessageLevel.Warning)

        epsg = self.requested_srid or self.data_srid or 4326     # SRID 0 = undeclared: assume lon/lat
        crs = QgsCoordinateReferenceSystem(f"EPSG:{epsg}")
        self.layer_crs = crs if crs.isValid() else QgsCoordinateReferenceSystem("EPSG:4326")

    # -- Features -------------------------------------------------------

    @staticmethod
    def _make_fid(attrs, wkb):
        """Stable 62-bit id from the row's content (same row -> same id)."""
        digest = hashlib.blake2b(repr(attrs).encode() + bytes(wkb or b""), digest_size=8).digest()
        return int.from_bytes(digest, "big") & ((1 << 62) - 1)

    def fetch_rows(self, request):
        """Rows for a feature request: [(fid, attrs, wkb), ...]."""
        if self.connection is None:
            return []
        filter_type = request.filterType()
        if filter_type in (QgsFeatureRequest.FilterType.Fid, QgsFeatureRequest.FilterType.Fids):
            wanted = [request.filterFid()] if filter_type == QgsFeatureRequest.FilterType.Fid \
                else list(request.filterFids())
            with self._lock:
                hits = [(fid,) + self._fid_cache[fid] for fid in wanted if fid in self._fid_cache]
            if len(hits) == len(wanted):
                return hits
            rows = self._select(None, 0, need_geometry=True)      # not cached: scan once
            wanted_set = set(wanted)
            return [r for r in rows if r[0] in wanted_set]

        rect = request.filterRect()
        no_geom = bool(request.flags() & QgsFeatureRequest.Flag.NoGeometry) and rect.isEmpty()
        return self._select(rect if not rect.isEmpty() else None, request.limit(),
                            need_geometry=not no_geom)

    def _select(self, rect, limit, need_geometry=True):
        names = [quote_identifier(f.name()) for f in self.fields_cache]
        geom_sql = None
        if self.geometry_column:
            geom_sql = quote_identifier(self.geometry_column)
        cols = names[:]
        if geom_sql and need_geometry:
            cols.append(f"ST_ASWKB({geom_sql})")
        statement = f"SELECT {', '.join(cols) or '1'} FROM {self.source_sql()}"  # nosec B608 - identifiers backtick-quoted; source is a quoted table or the user's own SELECT
        params = {}
        if rect is not None and geom_sql:
            statement += f" WHERE ST_INTERSECTS({geom_sql}, ST_GEOMFROMTEXT(:dbx_bbox, :dbx_srid))"
            params = {"dbx_bbox": rect.asWktPolygon(), "dbx_srid": int(self.data_srid)}
        if limit and limit > 0:
            statement += f" LIMIT {int(limit)}"
        try:
            raw = self._query(statement, params or None)
        except Exception as exc:
            _log(f"Error fetching features: {exc}", Qgis.MessageLevel.Critical)
            return []
        n_attrs = len(names)
        rows = []
        with self._lock:
            for record in raw:
                attrs = [_coerce(v) for v in record[:n_attrs]]
                wkb = record[n_attrs] if (geom_sql and need_geometry and len(record) > n_attrs) else None
                fid = self._make_fid(attrs, wkb)
                rows.append((fid, attrs, wkb))
                if wkb is not None:
                    self._fid_cache[fid] = (attrs, wkb)
                    self._fid_cache.move_to_end(fid)
            while len(self._fid_cache) > _FID_CACHE_SIZE:
                self._fid_cache.popitem(last=False)
        return rows

    # -- QgsVectorDataProvider interface -----------------------------------

    def featureSource(self):
        return DatabricksFeatureSource(self)

    def getFeatures(self, request=QgsFeatureRequest()):
        return QgsFeatureIterator(DatabricksFeatureIterator(DatabricksFeatureSource(self), request))

    def capabilities(self):
        return QgsVectorDataProvider.Capability.SelectAtId

    def fields(self):
        return self.fields_cache

    def featureCount(self):
        return self.feature_count_cache

    def wkbType(self):
        return self.geometry_type

    def extent(self):
        return self.extent_cache

    def updateExtents(self):
        pass

    def isValid(self):
        return self.connection is not None and self.is_valid_config()

    def name(self):
        return self.PROVIDER_KEY

    def description(self):
        return self.PROVIDER_DESCRIPTION

    def dataSourceUri(self, expandAuthConfig=False):
        return self.uri_string

    def crs(self):
        return self.layer_crs

    def reloadData(self):
        """Layer > Refresh: forget cached features and re-read stats."""
        with self._lock:
            self._fid_cache.clear()
        if self.connection is not None:
            self._initialize_layer()


def _coerce(value):
    """Convert driver values to types QgsFeature attributes accept."""
    import datetime as _dt
    import decimal as _dec
    if isinstance(value, _dec.Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, (list, dict)):
        return str(value)
    if isinstance(value, _dt.datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value


class DatabricksProviderMetadata(QgsProviderMetadata):
    """Provider metadata for Databricks provider"""

    def __init__(self):
        super().__init__(
            DatabricksProvider.PROVIDER_KEY,
            DatabricksProvider.PROVIDER_DESCRIPTION
        )

    def createProvider(self, uri, options, flags=None):
        """Create provider instance"""
        return DatabricksProvider(uri, options, flags)
