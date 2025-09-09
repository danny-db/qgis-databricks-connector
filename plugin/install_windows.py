"""
Cross-platform installation script for QGIS Databricks Connector
Optimized for Windows with fallback support for Linux
"""
import os
import sys
import subprocess
import shutil
from pathlib import Path
import glob

# Windows-specific imports
try:
    import winreg
    WINREG_AVAILABLE = True
except ImportError:
    WINREG_AVAILABLE = False


def get_qgis_python_executable():
    """Find the correct Python executable for QGIS on Windows"""
    if not sys.platform.startswith('win'):
        # For non-Windows systems, use system Python
        return sys.executable
    
    print("Detecting QGIS Python executable on Windows...")
    
    # Common QGIS installation paths on Windows
    possible_paths = []
    
    # Try to find QGIS installation from registry
    if WINREG_AVAILABLE:
        try:
            # Check for QGIS in registry (common installation method)
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\QGIS") as key:
                qgis_path = winreg.QueryValueEx(key, "InstallPath")[0]
                possible_paths.extend([
                    os.path.join(qgis_path, "bin", "python-qgis.bat"),
                    os.path.join(qgis_path, "bin", "python3.exe"),
                    os.path.join(qgis_path, "apps", "Python39", "python.exe"),
                    os.path.join(qgis_path, "apps", "Python38", "python.exe"),
                ])
        except (FileNotFoundError, OSError):
            pass
    
    # Common installation directories
    program_files = [
        os.environ.get('PROGRAMFILES', 'C:\\Program Files'),
        os.environ.get('PROGRAMFILES(X86)', 'C:\\Program Files (x86)')
    ]
    
    for pf in program_files:
        # Look for QGIS installations
        qgis_dirs = glob.glob(os.path.join(pf, "QGIS*"))
        for qgis_dir in qgis_dirs:
            possible_paths.extend([
                os.path.join(qgis_dir, "bin", "python-qgis.bat"),
                os.path.join(qgis_dir, "bin", "python3.exe"),
                os.path.join(qgis_dir, "apps", "Python39", "python.exe"),
                os.path.join(qgis_dir, "apps", "Python38", "python.exe"),
                os.path.join(qgis_dir, "apps", "Python37", "python.exe"),
            ])
    
    # OSGeo4W installations
    osgeo_paths = [
        "C:\\OSGeo4W64\\bin\\python-qgis.bat",
        "C:\\OSGeo4W64\\apps\\Python39\\python.exe",
        "C:\\OSGeo4W64\\apps\\Python38\\python.exe",
        "C:\\OSGeo4W\\bin\\python-qgis.bat",
        "C:\\OSGeo4W\\apps\\Python39\\python.exe",
    ]
    possible_paths.extend(osgeo_paths)
    
    # Test each path
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Found QGIS Python: {path}")
            return path
    
    return None


def install_dependencies():
    """Install required Python packages"""
    print("Installing Python dependencies...")
    
    # Find QGIS Python executable
    python_exe = get_qgis_python_executable()
    if not python_exe:
        print("Could not find QGIS Python executable. Skipping automatic installation.")
        print_manual_install_instructions()
        return False
    
    print(f"Using Python: {python_exe}")
    
    dependencies = [
        'databricks-sql-connector>=3.5.0',
        'shapely>=2.0.0'
    ]
    
    for dep in dependencies:
        try:
            # Try different methods based on the Python executable type
            if python_exe.endswith('.bat'):
                # Method 1: Try using python-qgis.bat directly with pip
                try:
                    cmd = [python_exe, '-m', 'pip', 'install', dep]
                    subprocess.check_call(cmd, shell=False, creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0)
                    print(f"✓ Installed {dep}")
                    continue
                except:
                    pass
                
                # Method 2: Try using OSGeo4W shell
                try:
                    osgeo_shell = python_exe.replace('python-qgis.bat', 'o4w_env.bat')
                    if os.path.exists(osgeo_shell):
                        cmd = [osgeo_shell, '&&', 'pip', 'install', dep]
                        subprocess.check_call(' '.join(cmd), shell=True)
                        print(f"✓ Installed {dep}")
                        continue
                except:
                    pass
            else:
                # Regular python executable
                cmd = [python_exe, '-m', 'pip', 'install', dep]
                subprocess.check_call(cmd)
                print(f"✓ Installed {dep}")
                continue
                
            # If we get here, all methods failed
            raise Exception("All installation methods failed")
            
        except Exception as e:
            print(f"✗ Failed to install {dep}: {e}")
            print("Will provide manual installation instructions.")
            print_manual_install_instructions()
            return False
    
    return True

def print_manual_install_instructions():
    """Print manual installation instructions"""
    print("\nTo install dependencies manually:")
    print("1. Open QGIS")
    print("2. Go to Plugins → Python Console")
    print("3. Run these commands:")
    print("   import subprocess, sys")
    print("   subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'databricks-sql-connector'])")
    print("   subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'shapely'])")
    print("4. Restart QGIS")


def get_qgis_plugin_directory():
    """Get the QGIS plugin directory for the current user"""
    home = Path.home()
    
    if sys.platform.startswith('win'):
        # Windows - try multiple possible locations
        possible_dirs = [
            home / 'AppData' / 'Roaming' / 'QGIS' / 'QGIS3' / 'profiles' / 'default' / 'python' / 'plugins',
        ]
        
        # Try alternative username detection methods
        try:
            username = os.getlogin()
            possible_dirs.append(
                Path('C:') / 'Users' / username / 'AppData' / 'Roaming' / 'QGIS' / 'QGIS3' / 'profiles' / 'default' / 'python' / 'plugins'
            )
        except OSError:
            # Fallback if os.getlogin() fails
            username = os.environ.get('USERNAME', os.environ.get('USER', 'default'))
            possible_dirs.append(
                Path('C:') / 'Users' / username / 'AppData' / 'Roaming' / 'QGIS' / 'QGIS3' / 'profiles' / 'default' / 'python' / 'plugins'
            )
        
        # Return the first existing directory, or the first one if none exist
        for plugin_dir in possible_dirs:
            if plugin_dir.parent.exists():
                return plugin_dir
        return possible_dirs[0]
        
    elif sys.platform.startswith('darwin'):
        # macOS
        return home / 'Library' / 'Application Support' / 'QGIS' / 'QGIS3' / 'profiles' / 'default' / 'python' / 'plugins'
    else:
        # Linux
        return home / '.local' / 'share' / 'QGIS' / 'QGIS3' / 'profiles' / 'default' / 'python' / 'plugins'


def install_plugin():
    """Install the plugin to QGIS plugins directory"""
    print("Installing QGIS plugin...")
    
    # Get plugin directory
    plugin_dir = get_qgis_plugin_directory()
    target_dir = plugin_dir / 'databricks_connector'
    
    print(f"Plugin directory: {plugin_dir}")
    
    # Create directories if they don't exist
    try:
        plugin_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"✗ Failed to create plugin directory: {e}")
        return False
    
    # Copy plugin files
    current_dir = Path(__file__).parent
    
    plugin_files = [
        '__init__.py',
        'metadata.txt',
        'databricks_connector.py',
        'databricks_provider.py',
        'databricks_dialog.py',
        'databricks_browser.py',
        '../README.md',
        'requirements.txt'
    ]
    
    # Remove existing plugin directory
    if target_dir.exists():
        try:
            shutil.rmtree(target_dir)
        except Exception as e:
            print(f"✗ Failed to remove existing plugin directory: {e}")
            return False
    
    try:
        target_dir.mkdir()
    except Exception as e:
        print(f"✗ Failed to create target directory: {e}")
        return False
    
    # Copy files
    copied_files = 0
    for file in plugin_files:
        src_file = current_dir / file
        if src_file.exists():
            try:
                shutil.copy2(src_file, target_dir / file)
                print(f"✓ Copied {file}")
                copied_files += 1
            except Exception as e:
                print(f"✗ Failed to copy {file}: {e}")
        else:
            print(f"✗ File not found: {file}")
    
    # Copy custom icons directory
    icons_src = current_dir / 'icons'
    if icons_src.exists() and icons_src.is_dir():
        try:
            icons_dest = target_dir / 'icons'
            shutil.copytree(icons_src, icons_dest)
            icon_count = len(list(icons_dest.glob('*.svg')))
            print(f"✓ Copied custom icons directory ({icon_count} SVG files)")
            copied_files += 1
        except Exception as e:
            print(f"✗ Failed to copy icons directory: {e}")
    else:
        print(f"✗ Icons directory not found: {icons_src}")
    
    print(f"Plugin installed to: {target_dir}")
    print(f"Copied {copied_files} files/directories")
    return copied_files > 0


def detect_platform():
    """Detect the current platform and return a friendly name"""
    if sys.platform.startswith('win'):
        return "Windows"
    elif sys.platform.startswith('darwin'):
        return "macOS"
    elif sys.platform.startswith('linux'):
        return "Linux"
    else:
        return f"Unknown ({sys.platform})"


def main():
    """Main installation function"""
    platform = detect_platform()
    print(f"QGIS Databricks DBSQL Connector - Installation Script for {platform}")
    print("=" * 70)
    
    # Install plugin first (works without dependencies)
    if not install_plugin():
        print("Failed to install plugin files.")
        return False
    
    # Install dependencies
    deps_success = install_dependencies()
    
    print("\n" + "=" * 70)
    if deps_success:
        print("✓ Installation completed successfully!")
        print("\nNext steps:")
        print("1. Restart QGIS")
        print("2. Go to Plugins → Manage and Install Plugins")
        print("3. Enable 'Databricks DBSQL Connector'")
        print("4. Click the Databricks icon in the toolbar to connect")
    else:
        print("⚠ Plugin installed but dependencies may be missing.")
        print_manual_install_instructions()
        
        if platform == "Windows":
            print("\nWindows-specific notes:")
            print("- If you have OSGeo4W installation, you may need to run the OSGeo4W Shell as Administrator")
            print("- Some Windows installations require using the QGIS Python Console method")
            print("- You can also try installing dependencies after QGIS is running")
    
    return True


if __name__ == '__main__':
    main()