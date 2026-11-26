"""Entry point for `python -m finspeq`: print which command runs which process."""

import sys

USAGE = """Finspeq has one command per process:

  finspeq-offline   build-dataset, dataset-manifest, train, baselines, export
  finspeq-stream    simulate, worker, verify-replay, verify-state

Run either with --help. Without the console scripts, use
python -m finspeq.bootstrap.offline or python -m finspeq.bootstrap.streaming.
"""

if __name__ == "__main__":
    sys.stdout.write(USAGE)
    sys.exit(2)
