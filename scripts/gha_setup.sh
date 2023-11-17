#!/bin/bash

set -e

# Configure SBT options
[[ -z "${BEAM_RUNNERS}" ]]                   || echo "-DbeamRunners=$BEAM_RUNNERS" >> .sbtopts
[[ -z "${GOOGLE_PROJECT_ID}" ]]              || echo "-Dbigquery.project=$GOOGLE_PROJECT_ID" >> .sbtopts
[[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]] || echo "-Dbigquery.secret=$BEAM_RUNNERS" >> .sbtopts
[[ -z "${CLOUDSQL_SQLSERVER_PASSWORD}" ]]    || echo "-Dcloudsql.sqlserver.password=$BEAM_RUNNERS" >> .sbtopts
