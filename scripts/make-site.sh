#!/bin/bash

# generate new site locally
# target/site/index.html
# using scala 2.12.8 due to https://github.com/criteo/socco/pull/6
SOCCO=true sbt ++2.12.8 scio-examples/clean scio-examples/compile site/makeSite

# generate new site and push to GitHub
#SOCCO=true sbt scio-examples/clean scio-examples/compile site/ghpagesPushSite
