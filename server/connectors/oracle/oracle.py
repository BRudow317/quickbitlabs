from __future__ import annotations

import logging, argparse
from .Job import job_exec

logger= logging.getLogger(__name__)

_APOLLO_DESCRIPTION = """Apollo currently exists as a program intended to accept cli arguments as an automation pipeline from a csv into an oracle database. The program is meant to be called by olympus.py which configures the environment."""

def main() -> int:
    parser = argparse.ArgumentParser(prog='apollo', description=_APOLLO_DESCRIPTION, add_help=True)
    parser.add_argument('--source', required=True, type=str, help='Required: Path to file')
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true', default=False)
    parser.add_argument('--test', '-t', dest='test_run', action='store_true', default=False)
    parser.add_argument('--log-dir', default='sys.stdout', type=str, help='The folder where the log should be written')
    parser.add_argument('--main-dir', type=str, help='The parent folder for the log, error, and processed directories')
    parser.add_argument('--table', required=False, type=str, help='Oracle Table overriding csv name default')
    parser.add_argument('--schema', required=False, type=str, help='Oracle Schema overriding user default')
    parser.add_argument('--batch-size', type=int, required=False, help='Integer for batch sizes')
    parser.add_argument('--error-dir', type=str, required=False, help='Path to errored files')
    parser.add_argument('--processed-dir', type=str, required=False, help='Path to processed files')
    args = parser.parse_args()

    logger.info('Apollo Started')
    result = 1
    try:
        result = job_exec(args)
        if result == 0:
            return result
        raise Exception(f'Apollo.main() : Final Result: {result}')
    except KeyboardInterrupt:
        print('Aborted by user.')
        logger.warning('Aborted by user. Returning: 2')
        return 2
    except Exception as e:
        logger.error(f'Apollo.main() : Critical Error: {e}')
        raise
    return result

if __name__ == '__main__':
    raise SystemExit(main())
