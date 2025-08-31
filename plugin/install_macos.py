"""
Updated installation script for macOS QGIS
"""
import os
import sys
import subprocess
import shutil
from pathlib import Path


def get_qgis_python_executable():
    """Find the correct Python executable for QGIS on macOS"""
    possible_paths = [
        "/Applications/QGIS.app/Contents/MacOS/lib/python3.9/bin/python3",
        "/Applications/QGIS.app/Contents/MacOS/bin/python3",
        "/Applications/QGIS.app/Contents/Resources/python/bin/python3",
        "/Applications/QGIS.app/Contents/MacOS/bin/python3.9"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    return None


def install_dependencies():
    """Install required Python packages"""
    print("Installing Python dependencies...")
    
    # Find QGIS Python executable
    python_exe = get_qgis_python_executable()
    if not python_exe:
        print("Could not find QGIS Python executable. Try manual installation:")
        print("1. Open QGIS Python Console")
        print("2. Run: import subprocess, sys")
        print("3. Run: subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'databricks-sql-connector', 'shapely'])")
        return False
    
    print(f"Using Python: {python_exe}")
    
    dependencies = [
        'databricks-sql-connector>=3.5.0',
        'shapely>=2.0.0'
    ]
    
    for dep in dependencies:
        try:
            cmd = [python_exe, '-m', 'pip', 'install', dep]
            subprocess.check_call(cmd)
            print(f"✓ Installed {dep}")
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to install {dep}: {e}")
            print("Try installing manually in QGIS Python Console")
            return False
    
    return True


def get_qgis_plugin_directory():
    """Get the QGIS plugin directory for the current user"""
    home = Path.home()
    plugin_dir = home / 'Library' / 'Application Support' / 'QGIS' / 'QGIS3' / 'profiles' / 'default' / 'python' / 'plugins'
    return plugin_dir


def install_plugin():
    """Install the plugin to QGIS plugins directory"""
    print("Installing QGIS plugin...")
    
    # Get plugin directory
    plugin_dir = get_qgis_plugin_directory()
    target_dir = plugin_dir / 'databricks_connector'
    
    # Create directories if they don't exist
    plugin_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy plugin files
    current_dir = Path(__file__).parent
    
    plugin_files = [
        '__init__.py',
        'metadata.txt',
        'databricks_connector.py',
        'databricks_provider.py',
        'databricks_dialog.py',
        'README.md',
        'requirements.txt'
    ]
    
    # Remove existing plugin directory
    if target_dir.exists():
        shutil.rmtree(target_dir)
    
    target_dir.mkdir()
    
    # Copy files
    copied_files = 0
    for file in plugin_files:
        src_file = current_dir / file
        if src_file.exists():
            shutil.copy2(src_file, target_dir / file)
            print(f"✓ Copied {file}")
            copied_files += 1
        else:
            print(f"✗ File not found: {file}")
    
    print(f"Plugin installed to: {target_dir}")
    print(f"Copied {copied_files} files")
    return copied_files > 0


def main():
    """Main installation function"""
    print("QGIS Databricks DBSQL Connector - Installation Script for macOS")
    print("=" * 60)
    
    # Install plugin first (works without dependencies)
    if not install_plugin():
        print("Failed to install plugin files.")
        return False
    
    # Install dependencies
    deps_success = install_dependencies()
    
    print("\n" + "=" * 60)
    if deps_success:
        print("✓ Installation completed successfully!")
        print("\nNext steps:")
        print("1. Restart QGIS")
        print("2. Go to Plugins → Manage and Install Plugins")
        print("3. Enable 'Databricks DBSQL Connector'")
        print("4. Click the Databricks icon in the toolbar to connect")
    else:
        print("⚠ Plugin installed but dependencies may be missing.")
        print("\nTo install dependencies manually:")
        print("1. Open QGIS")
        print("2. Go to Plugins → Python Console")
        print("3. Run these commands:")
        print("   import subprocess, sys")
        print("   subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'databricks-sql-connector'])")
        print("   subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'shapely'])")
        print("4. Restart QGIS")
    
    return True


if __name__ == '__main__':
    main()