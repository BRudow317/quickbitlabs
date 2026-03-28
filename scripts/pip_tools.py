import sys
import subprocess
import os
# VENV & ENVIRONMENT SETUP
def setup_venv(
    venv_path: str = ".venv",
    packages: list[str] | None = None,
    requirements_file: str | None = None,
    python_version: str | None = None
) -> bool:
    """
    setup_venv - Create virtual environment and install packages
    
    Args:
        venv_path: Path for the virtual environment
        packages: List of packages to install
        requirements_file: Path to requirements.txt
        python_version: Specific Python version (e.g., "python3.11")
    
    Returns:
        bool: Success status
    
    Example:
        setup_venv(".venv", ["fastapi", "uvicorn", "flask"])
        setup_venv(requirements_file="requirements.txt")
    """
    default_packages = [
        "fastapi",
        "uvicorn[standard]",
        "flask",
        "requests",
        "pydantic",
        "python-dotenv",
        "pyyaml",
        "toml",
        "xmltodict",
        "pandas",
        "openpyxl",
        "markdown",
        "beautifulsoup4",
        "lxml",
        "rich",
        "httpx",
        "python-multipart",
    ]
    
    packages = packages or default_packages
    python_cmd = python_version or sys.executable
    
    print(f"Creating virtual environment at {venv_path}...")
    
    try:
        # Create venv
        subprocess.run([python_cmd, "-m", "venv", venv_path], check=True)
        
        # Determine pip path
        if sys.platform == "win32":
            pip_path = os.path.join(venv_path, "Scripts", "pip")
            python_path = os.path.join(venv_path, "Scripts", "python")
        else:
            pip_path = os.path.join(venv_path, "bin", "pip")
            python_path = os.path.join(venv_path, "bin", "python")
        
        # Upgrade pip
        print("Upgrading pip...")
        subprocess.run([pip_path, "install", "--upgrade", "pip"], check=True)
        
        # Install from requirements file if provided
        if requirements_file and os.path.exists(requirements_file):
            print(f"Installing from {requirements_file}...")
            subprocess.run([pip_path, "install", "-r", requirements_file], check=True)
        
        # Install packages
        if packages:
            print(f"Installing {len(packages)} packages...")
            subprocess.run([pip_path, "install"] + packages, check=True)
        
        print(f"""
 Virtual environment created successfully!

To activate:
  Windows:   {venv_path}\\Scripts\\activate
  Linux/Mac: source {venv_path}/bin/activate

Python path: {python_path}
Pip path:    {pip_path}
""")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f" Error setting up venv: {e}")
        return False


def generate_requirements(
    output_file: str = "requirements.txt",
    include_versions: bool = True,
    exclude: list[str] | None = None
) -> str:
    """
    generate_requirements - Generate requirements.txt from current environment
    
    Args:
        output_file: Output file path
        include_versions: Include version pinning
        exclude: Packages to exclude
    
    Returns:
        str: Contents of requirements file
    
    Example:
        generate_requirements("requirements.txt")
        generate_requirements(include_versions=False)
    """
    exclude = exclude or []
    
    result = subprocess.run(
        [sys.executable, "-m", "pip", "freeze"],
        capture_output=True,
        text=True
    )
    
    lines = []
    for line in result.stdout.strip().split("\n"):
        if not line or line.startswith("#"):
            continue
        
        pkg_name = line.split("==")[0].lower()
        if pkg_name in [e.lower() for e in exclude]:
            continue
        
        if include_versions:
            lines.append(line)
        else:
            lines.append(pkg_name)
    
    content = "\n".join(sorted(lines))
    
    with open(output_file, "w") as f:
        f.write(content)
    
    print(f" Generated {output_file} with {len(lines)} packages")
    return content


def check_dependencies(packages: list[str]) -> dict[str, bool]:
    """
    check_dependencies - Check if packages are installed
    
    Args:
        packages: List of package names to check
    
    Returns:
        Dict mapping package names to installed status
    
    Example:
        status = check_dependencies(["fastapi", "flask", "numpy"])
        missing = [pkg for pkg, installed in status.items() if not installed]
    """
    import importlib.util
    
    results = {}
    for package in packages:
        # Handle packages with different import names
        import_name = package.split("[")[0].replace("-", "_").lower()
        spec = importlib.util.find_spec(import_name)
        results[package] = spec is not None
    
    return results