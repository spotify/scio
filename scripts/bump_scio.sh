#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

# Update scioVersion in downstream repos to the latest release
# To run this script, you should
# - have hub installed
# - have push permission to all $REPOS

REPOS=(
  'git@github.com:spotify/big-data-rosetta-code.git'
  'git@github.com:spotify/featran.git'
  'git@github.com:spotify/ratatool.git'
  'git@github.com:spotify/scio-contrib.git'
  'git@github.com:spotify/scio.g8.git'
)

if ! [ -x "$(command -v hub)" ]; then
  echo 'Error: hub is not installed'
  exit 1
fi

URL="https://oss.sonatype.org/content/repositories/releases/com/spotify/scio-core_2.11/maven-metadata.xml"
VERSION=$(curl -s $URL | grep -o '<latest>[^<>]\+</latest>' | sed -E 's/<\/?latest>//g')
echo "Latest Scio release: $VERSION"

make_pr() {
  REPO=$1
  DIR=$(basename $REPO | sed -e 's/\.git$//')
  echo
  echo "========================================"
  echo
  echo "Updating $REPO"
  git clone -q --depth 10 $REPO

  cd $DIR

  for FILE in $(git grep -l 'val scioVersion = "'); do
    echo "Updating $FILE"
    # sed -i behaves differently on Mac and Linux
    cat $FILE | sed "s/val scioVersion = \".*/val scioVersion = \"$VERSION\"/g" > $FILE.tmp
    mv $FILE.tmp $FILE
  done

  git update-index -q --refresh
  STATUS=0
  git diff-index --quiet HEAD -- || STATUS=$?
  if [ $STATUS -ne 0 ]; then
    echo "Submitting PR to $STATUS $REPO"
    git commit -a -m "Bump Scio to $VERSION"
    git push -u origin "HEAD:$(whoami)/scio-$VERSION"
    hub pull-request -f -m "Bump Scio to $VERSION"
  else
    echo "Failed to update $REPO"
  fi

  cd ..
}

TEMP=$(mktemp -d)
echo "Using temporary directory $TEMP"

cd $TEMP
for REPO in "${REPOS[@]}"; do
  make_pr $REPO
done
rm -rf $TEMP
