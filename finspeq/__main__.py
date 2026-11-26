"""Entry point for `python -m finspeq`."""

import sys

from finspeq.bootstrap import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
