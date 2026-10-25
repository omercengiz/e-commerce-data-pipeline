#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
for job in bronze_clickstream bronze_product bronze_order silver_clickstream silver_order gold_problem1 gold_problem2; do
    python -m "src.jobs.$job"
done
