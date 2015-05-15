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
    iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "remove(foo)"
    if [ $status != 0 ]; then
        echo "Error occured: " $status "at location: " $location
    else
        echo "Success"
    fi
    exit $status
}


# setup the array with four versions and record the UAID
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "create array foo <a:int32> [I=0:50000,1000,0]"
if [[ $? != 0 ]] ; then cleanup 1 0; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store(build(foo,1),foo)"
if [[ $? != 0 ]] ; then cleanup 1 0; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store(build(foo,2),foo)"
if [[ $? != 0 ]] ; then cleanup 1 0; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store(build(foo,3),foo)"
if [[ $? != 0 ]] ; then cleanup 1 0; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "store(build(foo,4),foo)"
if [[ $? != 0 ]] ; then cleanup 1 0; fi

uaid=`iquery -ocsv -c $IQUERY_HOST -p $IQUERY_PORT -aq "project(filter(list('arrays'),name='foo'),uaid)" | 
         sed 1d`

# calculate the free and used bytes for instance 0
free=`iquery -ocsv -c $IQUERY_HOST -p $IQUERY_PORT -aq "filter(list('datastores'), uaid=$uaid and inst=0)" |
         sed 1d | cut -d ',' -f 5`
used=`iquery -ocsv -c $IQUERY_HOST -p $IQUERY_PORT -aq "filter(list('datastores'), uaid=$uaid and inst=0)" |
         sed 1d | cut -d ',' -f 4`

# run remove_versions and verify that the correct versions are removed
iquery -c $IQUERY_HOST -p $IQUERY_PORT -naq "remove_versions(foo, 3)"
if [[ $? != 0 ]]; then cleanup 1 2; fi

count1=`iquery -ocsv -c $IQUERY_HOST -p $IQUERY_PORT -aq "aggregate(filter(foo, a = 4), count(a))" |
           sed 1d`
count2=`iquery -ocsv -c $IQUERY_HOST -p $IQUERY_PORT -aq "aggregate(filter(foo@3, a = 3), count(a))" |
           sed 1d`
if [[ $count1 != 50001 || $count2 != 50001 ]]; then cleanup 1 3; fi

iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "aggregate(filter(foo@2, a = 2), count(a))" 2>&1 |
    grep ARRAY_DOESNT_EXIST
if [[ $? != 0 ]]; then cleanup 1 4; fi
iquery -c $IQUERY_HOST -p $IQUERY_PORT -aq "aggregate(filter(foo@1, a = 1), count(a))" 2>&1 |
    grep ARRAY_DOESNT_EXIST
if [[ $? != 0 ]]; then cleanup 1 4; fi

# verify that the free space increased and the used bytes decreased
free1=`iquery -ocsv -c $IQUERY_HOST -p $IQUERY_PORT -aq "filter(list('datastores'), uaid=$uaid and inst=0)" |
         sed 1d | cut -d ',' -f 3`
used1=`iquery -ocsv -c $IQUERY_HOST -p $IQUERY_PORT -aq "filter(list('datastores'), uaid=$uaid and inst=0)" |
         sed 1d | cut -d ',' -f 2`

if [[ $free -ge $free1 ]]; then echo free $free free1 $free1 cleanup 1 5; fi
if [[ $used -le $used1 ]]; then cleanup 1 6; fi

# success
cleanup 0
