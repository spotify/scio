#!/bin/bash

set -e

echo "Resorting cached BigQuery schemas"
mkdir -p .bigquery
cp -v scripts/bigquery/* .bigquery/
