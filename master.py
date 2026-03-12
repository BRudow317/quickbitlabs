#!/usr/bin/env python3
from __future__ import annotations

import sys, subprocess, logging
from typing import IO, TextIO
from utils.parse_args import parse_args
from utils.setup_logging import setup_logging
from utils.child_env_handlers import reexec_into_venv_if_needed, prepare_child
from utils.parse_config_file import parse_config_file


_PROGRAM_NAME='quickbitlabs'
logger = logging.getLogger(_PROGRAM_NAME)

def main():

    args, passthrough_args = parse_args(sys.argv[1:])
    setup_logging(args.log_dir, args.verbose, _PROGRAM_NAME)
    reexec_into_venv_if_needed(args)
    config_vars = parse_config_file(args.config, env=args.env) if args.config else {}
    cmd, child_env = prepare_child(args, config_vars)
    cmd.extend(passthrough_args)
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=child_env)
    assert process.stdout is not None
    assert process.stderr is not None

    def stream_pipe(pipe: IO[bytes], out_stream: TextIO) -> None:
        for line in iter(pipe.readline, b""):
            out_stream.write(line.decode("utf-8", errors="replace"))
            out_stream.flush()
        pipe.close()
    import threading
    t_out = threading.Thread(target=stream_pipe, args=(process.stdout, sys.stdout))
    t_err = threading.Thread(target=stream_pipe, args=(process.stderr, sys.stderr))
    t_out.start(); t_err.start(); t_out.join(); t_err.join()
    process.wait()
    sys.exit( process.returncode )

if __name__ == '__main__':
    main()