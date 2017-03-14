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

DIR_OF_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$CI_PULL_REQUEST" = "" ]; then
  echo "Running test for branch: $CIRCLE_BRANCH"
  openssl aes-256-cbc -d -in "$DIR_OF_SCRIPT/data-integration-test-2210ed0f609b.json.enc" -out "$DIR_OF_SCRIPT/data-integration-test-2210ed0f609b.json" -k $ENCRYPTKEY
  "$DIR_OF_SCRIPT/circleci_parallel_run.sh" 'sbt ++$CI_SCALA_VERSION -Dbigquery.project=data-integration-test '"-Dbigquery.secret=$DIR_OF_SCRIPT/data-integration-test-2210ed0f609b.json"' scalastyle coverage test it:test coverageReport'
else
  echo "Running test for PR: $CI_PULL_REQUEST"
  "$DIR_OF_SCRIPT/gen_tornado_schema.sh"
  "$DIR_OF_SCRIPT/circleci_parallel_run.sh" 'sbt ++$CI_SCALA_VERSION -Dbigquery.project=dummy-project scalastyle coverage test coverageReport coverageAggregate'
fi

"$DIR_OF_SCRIPT/circleci_parallel_run.sh" 'sbt ++$CI_SCALA_VERSION coverageAggregate'
