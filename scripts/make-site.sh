#!/bin/bash

# generate new site locally
# target/site/index.html
# SOCCO=true sbt scio-examples/clean scio-examples/compile site/makeSite

# generate new site and push to GitHub
SOCCO=true sbt scio-examples/clean scio-examples/compile site/ghpagesPushSite
