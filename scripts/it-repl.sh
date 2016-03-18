#!/bin/bash

SCALA_VERSION=${1}

echo "Will test scio REPL for scala ${SCALA_VERSION}"

sbt ++$SCALA_VERSION "project scio-repl" assembly

echo "Test scripts:"
find ./scio-repl/src/it/resources -type file

find ./scio-repl/src/it/resources -type file -exec sh -c "cat {} | java -jar ./scio-repl/target/scala-${SCALA_VERSION%.*}/scio-repl-*.jar" \;
