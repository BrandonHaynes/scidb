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


test_file()
{
	FILENAME=$1
	echo
	echo ">>>running $FILENAME"
	time iquery -a -f $FILENAME 2>&1
}

MYDIR=`dirname $0`
pushd $MYDIR > /dev/null

iquery -a -q "dimensions(opt_dense_quad)" > /dev/null
if [ $? -ne 0 ]; then
	echo "Test query fafled... Rerun create.sh?"
	exit 1
fi

for FILE in \
	subarray1.afl \
	subarray2.afl \
	subarray_nested.afl \
	filter.afl \
	between.afl \
	join.afl \
	join_between_lhs.afl \
	join_filter_rhs.afl	\
	join_filter_lhs.afl \
	join_filter2_rhs.afl \
	join_nested.afl \
	join_nested2_lucky.afl \
	join_nested2_unlucky.afl \
	join_nested_sg_override.afl 
do
	test_file $FILE
done 
	
