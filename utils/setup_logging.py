from __future__ import annotations

import sys, logging
from datetime import datetime
from pathlib import Path
import os

_program_name=os.getenv('PROGRAM_NAME', '')

def setup_logging(
        log_dir: str = "sys.stdout",
        verbose: bool = True,
        program_name: str = _program_name
        ) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger(program_name)
    logger.setLevel(level)
    logger.handlers.clear()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    if log_dir and log_dir != 'sys.stdout':
        path = Path(log_dir)
        path.mkdir(parents=True, exist_ok=True)
        logfile = path / f"{datetime.now():%Y_%m_%d_%H_%M_%S}_{program_name}.log"
        fh = logging.FileHandler(logfile, delay=True)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger