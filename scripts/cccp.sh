#!/bin/bash
# Chief Commit Cherry Picker

if [ $# -ne 2 ]; then
    echo "Usage: cccp.sh <src> <dst>"
    exit 1
fi

SRC=$1
DST=$2
git log --format='%ai%x09%s' $SRC | sort > _src.txt
git log --format='%h%x09%ai%x09%s' $SRC > _srch.txt
git log --format='%ai%x09%s' $DST | sort > _dst.txt
git log --format='%h%x09%ai%x09%s' $DST > _dsth.txt
diff _src.txt _dst.txt > _diff.txt

IFS=$'\n'
for l in $(grep '^< ' _diff.txt | cut -c 3-); do
    grep -F "$l" _srch.txt
done > _srco.txt
for l in $(grep '^> ' _diff.txt | cut -c 3-); do
    grep -F "$l" _dsth.txt
done > _dsto.txt

echo "================================================================================"
echo "Missing commits in $SRC"
echo "================================================================================"
cat _srco.txt | grep -v "Merge pull request #" | grep -v "Setting version to"
echo
echo "================================================================================"
echo "Commits only in $SRC"
echo "================================================================================"
cat _srco.txt
echo
echo "================================================================================"
echo "Commits only in $DST"
echo "================================================================================"
cat _dsto.txt

rm -f _src.txt _srch.txt _dst.txt _dsth.txt _diff.txt _srco.txt _dsto.txt
