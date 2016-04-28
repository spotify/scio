#!/bin/bash

# Cached schema for BigQueryType.fromQuery in TypedBigQueryTornadoes
mkdir -p .bigquery
cat << EOF > .bigquery/19c90907ad1509de8cd1707e0270a87c.schema.json
{
  "fields" : [ {
    "mode" : "NULLABLE",
    "name" : "tornado",
    "type" : "BOOLEAN"
  }, {
    "mode" : "NULLABLE",
    "name" : "month",
    "type" : "INTEGER"
  } ]
}
EOF
