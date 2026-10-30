#!/usr/bin/env bash
set -euo pipefail
# Deploys the DAG folder (DAGs and their SQL) to the composer-agent environment. Run after a merge.
gcloud storage rsync --recursive dags gs://us-central1-composer-agent-76861d07-bucket/dags
