#!/bin/bash

# Cached schema for BigQueryType.fromQuery in TypedBigQueryTornadoes
mkdir -p .bigquery
cat << EOF > .bigquery/cbe0eb9cf213ecff2092964f3859ee81.schema.json
{
  "fields" : [ {
    "mode" : "NULLABLE",
    "name" : "tornado",
    "type" : "BOOLEAN"
  }, {
    "mode" : "REQUIRED",
    "name" : "month",
    "type" : "INTEGER"
  } ]
}
EOF
