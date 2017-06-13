#!/bin/bash
#
#  Copyright 2017 Spotify AB.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

set -e

DIR_OF_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$CI_PULL_REQUEST" = "" ]; then
  echo "Running test for branch: $CIRCLE_BRANCH"
  JSON_KEY=$(basename $GOOGLE_APPLICATION_CREDENTIALS)
  openssl aes-256-cbc -d -in "$DIR_OF_SCRIPT/$JSON_KEY.enc" -out "$DIR_OF_SCRIPT/$JSON_KEY" -k $ENCRYPTION_KEY
  "$DIR_OF_SCRIPT/circleci_parallel_run.sh" 'sbt -Dbigquery.project=data-integration-test '"-Dbigquery.secret=$DIR_OF_SCRIPT/$JSON_KEY"' ++$CI_SCALA_VERSION scalastyle coverage test it:test coverageReport coverageAggregate'
else
  echo "Running test for PR: $CI_PULL_REQUEST"
  "$DIR_OF_SCRIPT/circleci_parallel_run.sh" 'sbt -Dbigquery.project=dummy-project ++$CI_SCALA_VERSION scalastyle coverage test coverageReport coverageAggregate'
fi
