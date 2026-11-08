"""`python -m cdc_es_sync ...` is the same as the `cdc-es-sync` command."""
import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
