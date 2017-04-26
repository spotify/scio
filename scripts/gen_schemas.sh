#!/bin/bash

set -e

# Cached schemas
mkdir -p .bigquery
cp scripts/bigquery/* .bigquery/
