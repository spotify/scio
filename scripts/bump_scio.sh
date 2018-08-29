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
SCIO_VERSION=$(curl -s $URL | grep -o '<latest>[^<>]\+</latest>' | sed -E 's/<\/?latest>//g')
echo "Latest Scio release: $SCIO_VERSION"

URL="https://raw.githubusercontent.com/spotify/scio/v$SCIO_VERSION/build.sbt"
BEAM_VERSION=$(curl -s $URL | grep -o '^val beamVersion = "[^"]\+"' | cut -d '"' -f 2)
echo "Matching Beam release: $BEAM_VERSION"

git_clone() {
  REPO=$1
  echo
  echo "========================================"
  echo
  echo "Cloning $REPO"
  git clone -q --depth 10 $REPO
}

make_pr() {
  REPO=$1
  git update-index -q --refresh
  STATUS=0
  git diff-index --quiet HEAD -- || STATUS=$?
  if [ $STATUS -ne 0 ]; then
    echo "Submitting PR to $REPO"
    git commit -a -m "Bump Scio to $SCIO_VERSION"
    BRANCH="$(whoami)/scio-$SCIO_VERSION"
    git push -u origin "HEAD:$BRANCH"
    hub pull-request -f -m "Bump Scio to $SCIO_VERSION" -h $BRANCH
  else
    echo "Failed to update $REPO"
  fi
}

TEMP=$(mktemp -d)
echo "Using temporary directory $TEMP"
cd $TEMP

########################################
# Update homebrew formula
########################################

URL="https://github.com/spotify/scio/releases/download/v$SCIO_VERSION/scio-repl-$SCIO_VERSION.jar"
SHASUM=$(curl -sL $URL | shasum -a 256 | awk '{print $1}')
REPO="git@github.com:spotify/homebrew-public.git"
git_clone $REPO
cd homebrew-public
# sed -i behaves differently on Mac and Linux
cat scio.rb | \
  sed "s/url \"[^\"]*\"/url \"${URL//\//\\/}\"/g" | \
  sed "s/sha256 \"[^\"]*\"/sha256 \"$SHASUM\"/g" > scio.rb.tmp
mv scio.rb.tmp scio.rb
make_pr $REPO
cd ..

########################################
# Update downstream repos
########################################

update_downstream() {
  REPO=$1
  git_clone $REPO

  DIR=$(basename $REPO | sed -e 's/\.git$//')
  cd $DIR
  for FILE in $(git grep -l 'val \(scio\|beam\)Version = "'); do
    echo "Updating $FILE"
    # sed -i behaves differently on Mac and Linux
    cat $FILE | \
      sed "s/val scioVersion = \"[^\"]*\"/val scioVersion = \"$SCIO_VERSION\"/g" | \
      sed "s/val beamVersion = \"[^\"]*\"/val beamVersion = \"$BEAM_VERSION\"/g" > $FILE.tmp
    mv $FILE.tmp $FILE
  done
  make_pr $REPO
  cd ..
}

for REPO in "${REPOS[@]}"; do
  update_downstream $REPO
done

########################################
# Clean up
########################################

cd ..
echo "Deleting temporary directory $TEMP"
rm -rf $TEMP
