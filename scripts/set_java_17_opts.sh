#!/bin/bash

set -e

cat <<EOF >>.jvmopts
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
EOF
