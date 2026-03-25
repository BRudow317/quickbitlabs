import sys
from pathlib import Path

def import_fix():
    FILE_DIR = Path(__file__).resolve()
    
    if str(FILE_DIR) not in sys.path:
        sys.path.insert(0, str(FILE_DIR))
def set_project_root():
    project_root_found = False
    project_root = None
    file_directory = Path(__file__).resolve().parent
    while not project_root_found:
        for f in file_directory.iterdir():
            if Path(f).is_file() and Path(f).name == "__init__.py":
                file_directory = file_directory.parent
                break
            else:
                project_root = file_directory
                project_root_found = True
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))