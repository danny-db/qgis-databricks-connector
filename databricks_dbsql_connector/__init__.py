"""
QGIS Databricks DBSQL Connector Plugin

This plugin provides direct connectivity to Databricks SQL warehouses,
allowing you to load and display geospatial data from Unity Catalog tables.
"""


def classFactory(iface):
    """Load DatabricksConnector class from databricks_connector module.

    This function is called by QGIS to load the plugin.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    :return: Plugin instance
    :rtype: DatabricksConnector
    """
    from .databricks_connector import DatabricksConnector
    return DatabricksConnector(iface)
