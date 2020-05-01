#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# generate new site locally
# /site/target/site/index.html
# using scala 2.12.8 for annotated examples due to https://github.com/criteo/socco/pull/6
SOCCO=true sbt ++2.12.8 scio-examples/clean scio-examples/compile
# using latest scala for scaladoc due to
# https://github.com/scala/bug/issues/11635
sbt ++2.12.11 site/makeSite

# push to GitHub
while true; do
    read -r -p "Push site to GitHub? (y/n)" yn
    case $yn in
    [Yy]*)
        sbt ++2.12.11 site/ghpagesPushSite
        break
        ;;
    [Nn]*) exit ;;
    *) echo "Please answer yes or no." ;;
    esac
done
