#!/bin/bash

set -e

# Configure SBT options
cat <<EOF >>.jvmopts
-Dbigquery.project=$GOOGLE_PROJECT_ID
-Dbigquery.secret=$GOOGLE_APPLICATION_CREDENTIALS
EOF