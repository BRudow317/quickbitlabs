import sys, subprocess, importlib
def install_package(package: str, upgrade: bool = False) -> bool:
    """install_package - Install a package at runtime using pip. So make sure you're in a venv and implement proper controls to avoid security, cache, and versioning issues."""
    cmd = [sys.executable, "-m", "pip", "install"]
    if upgrade:
        cmd.append("--upgrade")
    cmd.append(package)
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        print(f"ok")
        return True
    except subprocess.CalledProcessError as e:
        print(f"fail")
        return False

# example usage:
def import_markdown():
    try:
        import markdown
        return markdown
    except ImportError:
        print("Package 'markdown' is missing. Attempting runtime installation...")
        
        # 1. Install the package safely
        success = install_package("markdown==3.5.2") # ALWAYS pin versions in automation
        
        if not success:
            raise RuntimeError("Failed to install required package 'markdown'.")
            
        # CRITICAL: Force Python to rescan the site-packages directory
        importlib.invalidate_caches()
        
        # 3. Try the import again
        try:
            import markdown
            print("Successfully installed and loaded 'markdown'.")
            return markdown
        except ImportError as e:
            raise RuntimeError(f"Package installed, but Python still cannot find it: {e}")