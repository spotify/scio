#!/bin/bash

set -e

base_revision="$1"
revision="$2"

if git diff --name-only "$base_revision" "$revision" | grep -q -E "^site/"; then
  sbt -v $SBT_OPTS site/makeSite
else
  echo "Skipping 'sbt site/makeSite'. No changes found"
fi
