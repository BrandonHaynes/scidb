#!/bin/sh
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
#
~/Devel/trunk/tests/data_gen/gen_matrix -d 10 10 10 10 1.0 NG > /tmp/Array.data

~/Devel/trunk/tests/data_gen/gen_matrix -d 2 2 3 3 1.0 NG > /tmp/Array.data
#
./iquery -q "list('types')"
./iquery -q "CREATE ARRAY Test_10 < Num_One: int32, Num_Two: double > [ I=0:99,10,0, J=0:99,10,0 ]"
./iquery -q "load ('Test_10', '/tmp/Array.data')"
#
./iquery -q "CREATE ARRAY Test_09 < Num_One: int32, Num_Two: double > [ I=0:5,3,0, J=0:5,3,0 ]"
./iquery -q "load ('Test_09', '/tmp/Array.data')"
#
./iquery -q "count (scan ('Test_04'))"

./iquery -q "list ('arrays')"
