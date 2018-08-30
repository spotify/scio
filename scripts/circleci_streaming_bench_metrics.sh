#!/bin/bash

set -e

DIR_OF_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -n "$ENCRYPTION_KEY" ]; then
  JSON_KEY=$(basename $GOOGLE_APPLICATION_CREDENTIALS)
  openssl aes-256-cbc -d -in "$DIR_OF_SCRIPT/$JSON_KEY.enc" -out "$DIR_OF_SCRIPT/$JSON_KEY" -k $ENCRYPTION_KEY
fi

PROPS="-Dbigquery.project=data-integration-test -Dbigquery.secret=$DIR_OF_SCRIPT/$JSON_KEY"

sbt -v $PROPS ++$SCALA_VERSION "scio-test/it:runMain com.spotify.ScioStreamingBenchmarkMetrics --name=ci"
