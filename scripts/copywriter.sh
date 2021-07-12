#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0


# Copyright 2018-2021 VMware, Inc., Microsoft Inc., Carnegie Mellon University, ETH Zurich, and University of Washington
# SPDX-License-Identifier: BSD-2-Clause

DRY_RUN=
if [ "$1" == "-d" ]; then
    DRY_RUN=echo
    shift
fi

OLD_COPYRIGHT_NOTICE=""
COPYRIGHT_NOTICE="Copyright 2018-2021 VMware, Inc."
COPYING_PERMISSION_STATEMENT="SPDX-License-Identifier: Apache-2.0"

function starts_with_bang_line() {
    FILENAME="$1"
    head -n 1 "$FILENAME" | grep "^#!" > /dev/null 2>&1
    status=("${PIPESTATUS[@]}")
    if [ ${status[0]} != 0 ]; then
        return 2
    fi
    return ${status[1]}
}

#
# Quote strings for use as literals in sed s///
#

# SYNOPSIS
#   quoteRe <text>
quoteRe() { sed -e 's/[^^]/[&]/g; s/\^/\\^/g; $!a\'$'\n''\\n' <<<"$1" | tr -d '\n'; }

# SYNOPSIS
#  quoteSubst <text>
quoteSubst() {
  IFS= read -d '' -r < <(sed -e ':a' -e '$!{N;ba' -e '}' -e 's/[&/\]/\\&/g; s/\n/\\&/g' <<<"$1")
  printf %s "${REPLY%$'\n'}"
}

while [ $1 ]; do
    FULLPATH="$1"
    shift

    FILENAME=`basename "$FULLPATH"`
    TMPSUFFIX="${FILENAME##*.}"
    SUFFIX="${TMPSUFFIX,,}"
    COMMENT_PREFIX=""
    BANGLINES=0
    if [ "$SUFFIX" == "dfy" ]; then
        COMMENT_PREFIX="//"
    elif [ "$SUFFIX" == "c" ]; then
        COMMENT_PREFIX="//"
    elif [ "$SUFFIX" == "h" ]; then
        COMMENT_PREFIX="//"
    elif [ "$SUFFIX" == "cpp" ]; then
        COMMENT_PREFIX="//"
    elif [ "$SUFFIX" == "hpp" ]; then
        COMMENT_PREFIX="//"
    elif [ "$SUFFIX" == "cs" ]; then
        COMMENT_PREFIX="//"
    elif [ "$SUFFIX" == "rs" ]; then
        COMMENT_PREFIX="//"
    elif [ "$SUFFIX" == "vpr" ]; then
        COMMENT_PREFIX="//"
    elif [ "$SUFFIX" == "makefile" ]; then
        COMMENT_PREFIX="#"
    elif [ "$SUFFIX" == "sh" ]; then
        starts_with_bang_line "$FULLPATH"
        RESULT=$?
        if [ $RESULT == 0 ]; then
            BANGLINES=1
        elif [ $RESULT != 1 ]; then
            continue
        fi
        COMMENT_PREFIX="#"
    elif [ "$SUFFIX" == "py" ]; then
        starts_with_bang_line "$FULLPATH"
        RESULT=$?
        if [ $RESULT == 0 ]; then
            BANGLINES=1
        elif [ $RESULT != 1 ]; then
            continue
        fi
        COMMENT_PREFIX="#"
    fi

    if [ "$COMMENT_PREFIX" == "" ]; then
        continue
    fi

    grep -Fx -m 1 "$COMMENT_PREFIX $COPYRIGHT_NOTICE" "$FULLPATH" > /dev/null 2>&1
    CONTAINS_COPYRIGHT_NOTICE=$?

    CONTAINS_OLD_COPYRIGHT_NOTICE=1
    if [ "$OLD_COPYRIGHT_NOTICE" ]; then
        grep -Fx -m 1 "$COMMENT_PREFIX $OLD_COPYRIGHT_NOTICE" "$FULLPATH" > /dev/null 2>&1
        CONTAINS_OLD_COPYRIGHT_NOTICE=$?
    fi

    grep -Fx -m 1 "$COMMENT_PREFIX $COPYING_PERMISSION_STATEMENT" "$FULLPATH" > /dev/null 2>&1
    CONTAINS_COPYING_PERMISSION_STATEMENT=$?

    if [[ $CONTAINS_COPYING_PERMISSION_STATEMENT -gt 1 || $CONTAINS_OLD_COPYRIGHT_NOTICE -gt 1 || $CONTAINS_COPYRIGHT_NOTICE -gt 1 ]]; then
        echo "error checking for existing notices in $FULLPATH. Skipping."
    fi

    if [[ $CONTAINS_COPYRIGHT_NOTICE == 0 && $CONTAINS_OLD_COPYRIGHT_NOTICE == 1 && $CONTAINS_COPYING_PERMISSION_STATEMENT == 0 ]]; then
      continue
    elif [[ $CONTAINS_COPYRIGHT_NOTICE == 1 && $CONTAINS_OLD_COPYRIGHT_NOTICE == 1 && $CONTAINS_COPYING_PERMISSION_STATEMENT == 1 ]]; then
        TMPFILE=`mktemp` &&
        head -n $BANGLINES "$FULLPATH" > "$TMPFILE" &&
        if [ $BANGLINES -gt 0 ]; then echo >> "$TMPFILE"; fi &&
        echo "$COMMENT_PREFIX $COPYRIGHT_NOTICE" >> "$TMPFILE" &&
        echo "$COMMENT_PREFIX $COPYING_PERMISSION_STATEMENT" >> "$TMPFILE" &&
        echo >> "$TMPFILE" &&
        tail -n +"$[ $BANGLINES + 1 ]" "$FULLPATH" >> "$TMPFILE" &&
        $DRY_RUN cp "$TMPFILE" "$FULLPATH"
    elif [[ $CONTAINS_COPYRIGHT_NOTICE == 1 && $CONTAINS_OLD_COPYRIGHT_NOTICE == 0 && $CONTAINS_COPYING_PERMISSION_STATEMENT == 0 ]]; then
        $DRY_RUN sed -i "s/$(quoteRe "$COMMENT_PREFIX $OLD_COPYRIGHT_NOTICE")/$(quoteSubst "$COMMENT_PREFIX $COPYRIGHT_NOTICE")/" "$FULLPATH"
    else
        echo "$FULLPATH contains some weird combination of copyright notice, old copyright notice, and permission statement.  Skipping."
    fi

done
