#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# generate new site locally
# /site/target/site/index.html
SOCCO=true sbt scio-examples/compile site/makeSite

# push to GitHub
while true; do
    read -r -p "Push site to GitHub? (y/n)" yn
    case $yn in
    [Yy]*)
        sbt site/ghpagesPushSite
        break
        ;;
    [Nn]*) exit ;;
    *) echo "Please answer yes or no." ;;
    esac
done
