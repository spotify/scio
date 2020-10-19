#!/bin/bash

set -e

# Decrypt google credentials
openssl aes-256-cbc -d -salt -pbkdf2 \
  -in "$GOOGLE_APPLICATION_CREDENTIALS.enc" \
  -out "$GOOGLE_APPLICATION_CREDENTIALS" \
  -k "$ENCRYPTION_KEY"

# Configure SBT options
cat <<EOF >>.sbtops
-Dbigquery.project=$GOOGLE_PROJECT_ID
-Dbigquery.secret=$GOOGLE_APPLICATION_CREDENTIALS
EOF