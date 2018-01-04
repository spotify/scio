#!/bin/bash

set -e

URL="https://repository.apache.org/content/groups/snapshots/org/apache/beam/beam-runners-core-java/maven-metadata.xml"
VERSION=$(curl -s $URL | grep -o '<latest>[^<>]\+</latest>' | sed -E 's/<\/?latest>//g')
echo "Preparing build for Beam snapshot $VERSION"

git rebase master

PLATFORM=$(uname -s)
if [ "$PLATFORM" == "Darwin" ]; then
  sed -i '' "s/val beamVersion = \".*/val beamVersion = \"$VERSION\"/g" build.sbt
elif [ "$PLATFORM" == "Linux" ]; then
  sed -i "s/val beamVersion = \".*/val beamVersion = \"$VERSION\"/g" build.sbt
else
  echo "Unsupported platform: $PLATFORM"
  exit 1
fi
