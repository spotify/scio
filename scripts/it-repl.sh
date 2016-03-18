#!/bin/sh

SCALA_VERSION=${1?"provide scala version as first argument"}

sbt ++$SCALA_VERSION "project scio-repl" assembly
find scio-repl/src/it/resources -type file -exec sh -c "cat {} | java -jar scio-repl/target/scala-${SCALA_VERSION%.*}/scio-repl-*.jar" \;
