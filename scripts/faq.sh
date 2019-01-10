#!/bin/bash

TOKEN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/token.txt"
if [ ! -f "$TOKEN" ]; then
    echo "Cannot find GitHub application token $TOKEN"
    exit 1
fi

# filter out existing TOC
grep -A 1000000 "### General questions" FAQ.md > _body

# generate new TOC
./scripts/gh-md-toc _body | sed -e 's/^         //g' | grep -v '^Created by \[gh-md-toc\]' | grep -v '#table-of-contents' > _toc

# merge
cat _toc _body > _faq
rm _toc _body
mv _faq FAQ.md
