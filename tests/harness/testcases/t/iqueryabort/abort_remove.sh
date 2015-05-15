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

# remove arrays and exit with status provided
cleanup()
{
    location=$3
    iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "remove(fooar)"
    echo "Error occured: 1 at location: " $location
    exit 1
}

# create the test array along with four versions
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "create array fooar <a:int32> [I=0:50000,1000,0]"
if [[ $? != 0 ]] ; then cleanup 0; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store(build(fooar,1),fooar)"
if [[ $? != 0 ]] ; then clenaup 0; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store(build(fooar,2),fooar)"
if [[ $? != 0 ]] ; then clenaup 0; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store(build(fooar,3),fooar)"
if [[ $? != 0 ]] ; then clenaup 0; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store(build(fooar,4),fooar)"
if [[ $? != 0 ]] ; then clenaup 0; fi

uaid=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "project(filter(list('arrays'),name='fooar'),uaid)" | 
         grep \{0\} | cut -d ' ' -f 2`

# case 1 --- abort the remove of the first two versions of the array.
# Verify that the first two versions are actually removed
${TEST_UTILS_DIR}/killquery.sh -afl 2 0 'remove_versions(fooar, 3)'

iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "rename(fooar, fooar2)"
iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "rename(fooar2, fooar)"

lines=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "filter(list('arrays', true), uaid=$uaid)" | wc -l`
if [[ $lines != 4 ]]; then echo lines = $lines; cleanup 1; fi

count=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "aggregate(filter(fooar@3, a = 3), count(a))" |
         grep \{0\} | cut -d ' ' -f 2`
if [[ $count != 50001 ]]; then echo count = $count; cleanup 2; fi
count=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "aggregate(filter(fooar@4, a = 4), count(a))" |
         grep \{0\} | cut -d ' ' -f 2`
if [[ $count != 50001 ]]; then echo count = $count; cleanup 3; fi

# case 2 --- abort the complete removal of the array.
# Verify that the first two versions are actually removed
${TEST_UTILS_DIR}/killquery.sh -afl 2 0 'remove(fooar)'

lines=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "filter(list('arrays', true), uaid=$uaid)" | wc -l`
if [[ $lines != 1 ]]; then echo lines = $lines; cleanup 4; fi

# success
exit 0