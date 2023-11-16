#!/bin/bash

set -e

# Configure SBT options
cat <<EOF >>.sbtopts
-Dbigquery.project=$GOOGLE_PROJECT_ID
-Dbigquery.secret=$GOOGLE_APPLICATION_CREDENTIALS
-Dcloudsql.sqlserver.password=$CLOUDSQL_SQLSERVER_PASSWORD
EOF
