#!/bin/bash

set -e

# Configure SBT options
[[ -z "${BEAM_RUNNERS}" ]]                   || echo "-DbeamRunners=$BEAM_RUNNERS"                                >> .sbtopts
[[ -z "${GOOGLE_CLOUD_PROJECT}" ]]           || echo "-Dbigquery.project=$GOOGLE_CLOUD_PROJECT"                   >> .sbtopts
[[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]] || echo "-Dbigquery.secret=$GOOGLE_APPLICATION_CREDENTIALS"          >> .sbtopts
[[ -z "${CLOUDSQL_SQLSERVER_PASSWORD}" ]]    || echo "-Dcloudsql.sqlserver.password=$CLOUDSQL_SQLSERVER_PASSWORD" >> .sbtopts
