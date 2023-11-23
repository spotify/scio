#!/bin/bash

set -e

# Configure SBT options
[[ -z "${BEAM_RUNNERS}" ]]                   || echo "-DbeamRunners=$BEAM_RUNNERS"                                >> .sbtopts
[[ -z "${GOOGLE_CLOUD_PROJECT}" ]]           || echo "-Dbigquery.project=$GOOGLE_CLOUD_PROJECT"                   >> .sbtopts
[[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]] || echo "-Dbigquery.secret=$GOOGLE_APPLICATION_CREDENTIALS"          >> .sbtopts
[[ -z "${BQ_CONNECT_TIMEOUT}" ]]             || echo "-Dbigquery.connect_timeout=$BQ_CONNECT_TIMEOUT"             >> .sbtopts
[[ -z "${BQ_READ_TIMEOUT}" ]]                || echo "-Dbigquery.read_timeout=$BQ_READ_TIMEOUT"                   >> .sbtopts
[[ -z "${CLOUDSQL_SQLSERVER_PASSWORD}" ]]    || echo "-Dcloudsql.sqlserver.password=$CLOUDSQL_SQLSERVER_PASSWORD" >> .sbtopts
