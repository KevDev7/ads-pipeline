#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env.local ]]; then
  set -a
  source .env.local
  set +a
fi
: "${JAVA_HOME:?Set JAVA_HOME to a Java 17 or 21 installation (or in .env.local)}"
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
# A resource budget, not a query optimization. Keep it fixed when comparing runs.
export PYSPARK_SUBMIT_ARGS="--driver-memory 2g pyspark-shell"
uv run --frozen python experiments/04_ads_shuffle_join/run.py "$@"
