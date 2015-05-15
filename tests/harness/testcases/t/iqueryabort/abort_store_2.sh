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
    status=$1
    location=$2
    iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "remove(fooas2)"
    if [ $status != 0 ]; then
        echo "Error occured: " $status "at location: " $location
    else
        echo "Success"
    fi
    exit $status
}

# create the test array
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "create array fooas2 <v:int64> [I=0:2000,100,0]"
if [[ $? != 0 ]] ; then cleanup 1 1; fi

uaid=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "project(filter(list('arrays'),name='fooas2'),uaid)" | 
         grep \{0\} | cut -d ' ' -f 2`

# case 1 --- abort the store of the first version of an array.
# Verify that no datastore is created.
${TEST_UTILS_DIR}/killquery.sh -afl 2  2 'store (build (fooas2, I), fooas2)'
if [[ $? != 0 ]]; then cleanup 1 2; fi

iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "rename(fooas2, fooas2a)"
iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "rename(fooas2a, fooas2)"
lines=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "filter(list('datastores'), uaid=$uaid)" | wc -l`
if [[ $lines != 1 ]]; then echo lines = $lines; cleanup 1 3; fi

# case 2 --- abort the store of the second version of an array.
# Verify that the contents did not change and that the used size of the
# array did not increase
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store (build (fooas2, I), fooas2)"
if [[ $? != 0 ]] ; then cleanup 1 4; fi
size=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "project(filter(list('datastores'), uaid=$uaid), log_resv_bytes)" |
       awk '{ sum+=$2} END {print sum}'`

${TEST_UTILS_DIR}/killquery.sh -afl 2  2 'store (build (fooas2, I+1), fooas2)'
if [[ $? != 0 ]]; then cleanup 1 5; fi

lines=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "filter (fooas2, v = I)" | wc -l`
if [[ $lines != 2002 ]]; then echo lines = $lines; cleanup 1 6; fi

iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "rename(fooas2, fooas2a)"
iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "rename(fooas2a, fooas2)"
size1=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "project(filter(list('datastores'), uaid=$uaid), log_resv_bytes)" |
        awk '{ sum+=$2} END {print sum}'`
if [ $size != $size1 ]; then echo $size $size1; cleanup 1 7; fi

# success
cleanup 0 0