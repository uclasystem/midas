#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SRC_DIR=$( cd -- $SCRIPT_DIR/.. &> /dev/null && pwd )

make --always-make --dry-run \
 | grep -wE 'gcc|g\+\+' \
 | grep -w '\-c' \
 | jq -nR "[inputs|{directory:\"${SRC_DIR}\", command:., file: match(\" [^ ]+$\").string[1:]}]" \
 > compile_commands.json
