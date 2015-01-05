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

MYDIR=`dirname $0`
NUM_INSTANCES=4 #including coordinator
DB_NAME="mydb"
	
check_exit_status()
{
	if [ $1 -ne 0 ]; then 
		echo "Error above. Exiting. Peace."
		exit 1;
	fi
}

launch_db()
{
	pushd ../basic > /dev/null
	check_exit_status $?
	./runN.py $NUM_INSTANCES $DB_NAME init,start > /dev/null
	check_exit_status $?
	popd > /dev/null
}

pushd $MYDIR > /dev/null

#make sure db is up
iquery -a -q "list()" > /dev/null
if [ $? -ne 0 ]; then
	echo "Can't access db... trying to restart"
	killall scidb > /dev/null 2>&1
	launch_db
fi

rm -f distrotest.out

iquery -a -q "apply(tbt,instanceid,instanceid())" >> distrotest.out
check_exit_status $?

iquery -a -q "apply(sg(tbt,1,-1,foo,0,1,0,2,2),instanceid,instanceid())" >> distrotest.out
check_exit_status $?

iquery -a -q "apply(sg(tbt,1,-1,foo,0,0,1,2,2),instanceid,instanceid())" >> distrotest.out
check_exit_status $?

iquery -a -q "apply(sg(tbt,1,-1,foo,0,1,1,2,2),instanceid,instanceid())" >> distrotest.out
check_exit_status $?

iquery -a -q "apply(sg(fbf,1,-1,foo,0,0,0,4,4),instanceid,instanceid())" >> distrotest.out
check_exit_status $?
#does nothing (vector smaller than chunk size)
iquery -a -q "apply(sg(fbf,1,-1,foo,0,1,1,4,4),instanceid,instanceid())" >> distrotest.out
check_exit_status $?

#same as vector (2,0)
iquery -a -q "apply(sg(fbf,1,-1,foo,0,2,1,4,4),instanceid,instanceid())" >> distrotest.out
check_exit_status $?
iquery -a -q "apply(sg(fbf,1,-1,foo,0,2,0,4,4),instanceid,instanceid())" >> distrotest.out
check_exit_status $?

#equiv
iquery -a -q "apply(sg(fbf,1,-1,foo,0,-2,-2,4,4),instanceid,instanceid())" >> distrotest.out
check_exit_status $?
iquery -a -q "apply(sg(fbf,1,-1,foo,0,2, 2,4,4),instanceid,instanceid())" >> distrotest.out
check_exit_status $?

#proof that join only requires colocation, but not correct distribution
iquery -a -q "apply(join( sg(fbf,1,-1,foo1,0,2,2,4,4), sg(fbf,1,-2,foo2,0,2,2,4,4)), abc, foo1.val+foo2.val)" >> distrotest.out

diff distrotest.out distrotest.exp
check_exit_status $?
echo "All set"
