#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#
#

FAIL=0
for FILE in $* ; do
    echo "checking $FILE" >&2
    awk '
    BEGIN  { FAIL=0; }
           { if(length() >72) { FAIL=1; printf("line %s too long: %s\n",NR, $0); } }
    END    { exit(FAIL); }
    ' $FILE

    if [ "$?" -ne 0 ] ; then
        FAIL=1
    fi
done

exit $FAIL
