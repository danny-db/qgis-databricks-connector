#!/usr/bin/env python3
"""
Package the Databricks DBSQL Connector plugin for QGIS Plugin Repository.

This script creates a ZIP file suitable for upload to plugins.qgis.org.
The ZIP will contain the plugin folder as the root directory.

Usage:
    python3 package_plugin.py

Output:
    databricks_dbsql_connector.zip - Ready for upload to QGIS plugin repository
"""

import os
import zipfile
import shutil
from pathlib import Path

# Plugin folder name (must match the folder name in the repository)
PLUGIN_NAME = "databricks_dbsql_connector"

# Files and folders to include in the package
INCLUDE_FILES = [
    "__init__.py",
    "metadata.txt",
    "LICENSE",
    "requirements.txt",
    "databricks_connector.py",
    "databricks_dialog.py",
    "databricks_browser.py",
    "databricks_provider.py",
    "databricks_live_layer.py",
]

INCLUDE_FOLDERS = [
    "icons",
]

# Files and patterns to exclude
EXCLUDE_PATTERNS = [
    "__pycache__",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    ".DS_Store",
    "*.backup",
    "*.bak",
    "*~",
    ".git",
    ".gitignore",
]


def should_exclude(filename):
    """Check if a file should be excluded based on patterns."""
    import fnmatch
    for pattern in EXCLUDE_PATTERNS:
        if fnmatch.fnmatch(filename, pattern):
            return True
    return False


def create_plugin_zip():
    """Create the plugin ZIP file for QGIS repository upload."""
    script_dir = Path(__file__).parent
    plugin_dir = script_dir / PLUGIN_NAME
    output_zip = script_dir / f"{PLUGIN_NAME}.zip"
    
    # Check if plugin directory exists
    if not plugin_dir.exists():
        print(f"Error: Plugin directory '{PLUGIN_NAME}' not found!")
        print(f"Expected location: {plugin_dir}")
        return False
    
    # Remove existing ZIP if present
    if output_zip.exists():
        output_zip.unlink()
        print(f"Removed existing {output_zip.name}")
    
    print(f"Creating {output_zip.name}...")
    
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add individual files
        for filename in INCLUDE_FILES:
            file_path = plugin_dir / filename
            if file_path.exists():
                arcname = f"{PLUGIN_NAME}/{filename}"
                zipf.write(file_path, arcname)
                print(f"  Added: {arcname}")
            else:
                print(f"  Warning: {filename} not found, skipping...")
        
        # Add folders
        for folder in INCLUDE_FOLDERS:
            folder_path = plugin_dir / folder
            if folder_path.exists():
                for root, dirs, files in os.walk(folder_path):
                    # Remove excluded directories
                    dirs[:] = [d for d in dirs if not should_exclude(d)]
                    
                    for file in files:
                        if not should_exclude(file):
                            file_path = Path(root) / file
                            rel_path = file_path.relative_to(plugin_dir)
                            arcname = f"{PLUGIN_NAME}/{rel_path}"
                            zipf.write(file_path, arcname)
                            print(f"  Added: {arcname}")
            else:
                print(f"  Warning: Folder '{folder}' not found, skipping...")
    
    # Get ZIP file size
    zip_size = output_zip.stat().st_size
    zip_size_mb = zip_size / (1024 * 1024)
    
    print(f"\n✅ Successfully created: {output_zip.name}")
    print(f"   Size: {zip_size_mb:.2f} MB")
    
    if zip_size_mb > 25:
        print(f"\n⚠️  Warning: ZIP file is larger than 25MB limit for QGIS plugin repository!")
    else:
        print(f"\n✅ Size is within the 25MB limit for QGIS plugin repository.")
    
    print(f"\n📦 Next steps:")
    print(f"   1. Test the plugin locally: QGIS → Plugins → Install from ZIP")
    print(f"   2. Upload to: https://plugins.qgis.org/plugins/upload/")
    
    return True


if __name__ == "__main__":
    success = create_plugin_zip()
    exit(0 if success else 1)

