#!/bin/bash

# Cached schema for BigQueryType.fromQuery in TypedBigQueryTornadoes
mkdir -p .bigquery
cat << EOF > .bigquery/07fe023d0e229e12f83b3e39c0acf54a.schema.json
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
