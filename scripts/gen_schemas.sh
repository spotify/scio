#!/bin/bash

set -e

echo "Restoring cached BigQuery schemas"
mkdir -p .bigquery
cp -v scripts/bigquery/* .bigquery/
