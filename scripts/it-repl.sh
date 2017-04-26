#!/bin/bash

set -e

if [ $# != 1 ]; then
  echo "usage: $0 [full-scala-version]"
  echo "example: $0 2.11.8"
  exit 1
fi

SCALA_VERSION=$1

echo "Will test scio REPL for scala ${SCALA_VERSION}"

sbt ++$SCALA_VERSION "project scio-repl" coverage assembly

echo "Test scripts:"
find ./scio-repl/src/it/resources -type f

find ./scio-repl/src/it/resources -type f -exec sh -c "cat {} | java -jar ./scio-repl/target/scala-${SCALA_VERSION%.*}/scio-repl-*.jar" \;
