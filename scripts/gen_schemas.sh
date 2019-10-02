#!/bin/bash

set -e

echo "Restoring cached BigQuery schemas"

SCHEMAS_DIR=/tmp/scio-bigquery-$USER/.bigquery/

mkdir -p $SCHEMAS_DIR
cp -v scripts/bigquery/* $SCHEMAS_DIR
