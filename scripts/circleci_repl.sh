#!/bin/bash

set -e

echo "Running REPL integration tests for Scala $SCALA_VERSION"
echo "Test scripts:"
find ./scio-repl/src/it/resources -type f

sbt "++$SCALA_VERSION scio-repl/assembly"

REPL_JAR="./scio-repl/target/scala-${SCALA_VERSION%.*}/scio-repl-*.jar"

find ./scio-repl/src/it/resources -type f -exec sh -c "cat {} | java -jar $REPL_JAR" \; | tee repl-$SCALA_VERSION.log
grep 'SUCCESS: \[scio\]' repl-$SCALA_VERSION.log
