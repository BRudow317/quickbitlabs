import sys
from pathlib import Path

def import_fix():
    file_dir = Path(__file__).resolve().parent
    if str(file_dir) not in sys.path:
        sys.path.insert(0, str(file_dir))

def set_project_root():
    file_directory = Path(__file__).resolve().parent
    while (file_directory / "__init__.py").exists():
        file_directory = file_directory.parent
    if str(file_directory) not in sys.path:
        sys.path.insert(0, str(file_directory))