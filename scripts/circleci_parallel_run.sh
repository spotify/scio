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

case $CIRCLE_NODE_INDEX in
  0) export CI_SCALA_VERSION="2.11.8"
    ;;
  1) export CI_SCALA_VERSION="2.12.1"
    ;;
  *)
    exit 1
    ;;
esac

echo "INFO: scala version is $CI_SCALA_VERSION"
bash -c "$1"
