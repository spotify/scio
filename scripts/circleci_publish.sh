#!/bin/bash

set -e

if [ "$CIRCLE_BRANCH" == "nio" ]; then
    echo "Publishing from nio branch"
    sed -i "s/-SNAPSHOT/-nio-SNAPSHOT/g" version.sbt
fi
