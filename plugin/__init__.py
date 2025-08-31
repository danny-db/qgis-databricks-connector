"""
QGIS Databricks DBSQL Connector Plugin - Robust initialization
"""

def classFactory(iface):
    """Load DatabricksConnector class from file databricks_connector.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    try:
        from .databricks_connector import DatabricksConnector
        return DatabricksConnector(iface)
    except ImportError as e:
        # Log import error but don't crash
        print(f"Warning: Could not import Databricks Connector dependencies: {e}")
        # Return a minimal plugin that shows error message
        from .databricks_connector import DatabricksConnector
        return DatabricksConnector(iface)
    except Exception as e:
        # Log any other errors
        print(f"Error loading Databricks Connector: {e}")
        raise