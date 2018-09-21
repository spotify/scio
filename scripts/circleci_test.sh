#!/bin/bash

set -e

DIR_OF_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -n "$ENCRYPTION_KEY" ]; then
  JSON_KEY=$(basename $GOOGLE_APPLICATION_CREDENTIALS)
  openssl aes-256-cbc -d -in "$DIR_OF_SCRIPT/$JSON_KEY.enc" -out "$DIR_OF_SCRIPT/$JSON_KEY" -k $ENCRYPTION_KEY
fi

if [ -n "$CIRCLE_PR_USERNAME" ]; then
  echo "Running tests for Scala $SCALA_VERSION, forked PR #$CIRCLE_PR_NUMBER from $CIRCLE_PR_USERNAME/$CIRCLE_PR_REPONAME"
  PROPS="-Dbigquery.project=dummy-project"
  TESTS="test"
else
  echo "Running tests for Scala $SCALA_VERSION, branch: $CIRCLE_BRANCH"
  PROPS="-Dbigquery.project=data-integration-test -Dbigquery.secret=$DIR_OF_SCRIPT/$JSON_KEY"
  TESTS="test it:test"
fi

"$DIR_OF_SCRIPT/gen_schemas.sh"

if [ $SCOVERAGE -eq 1 ]; then
  sbt -v $PROPS ++$SCALA_VERSION \
    scalafmtCheck test:scalafmtCheck scalafmtSbtCheck \
    scalastyle \
    coverage \
    $TESTS \
    coverageReport coverageAggregate
else
  sbt -v $PROPS ++$SCALA_VERSION \
    scalafmtCheck test:scalafmtCheck scalafmtSbtCheck \
    scalastyle \
    $TESTS
fi
