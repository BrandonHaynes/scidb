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
# Tested on 2-instance cluster

function tiq() {
    cmd=$1
    silent="-n"
    afl="--afl"
    echo -n $cmd 
    /usr/bin/time -p -f "delta %E, status %x" -o /tmp/delta iquery -c 10.0.0.1 $afl $silent -r /tmp/res -q "$cmd"
    grep "delta" /tmp/delta
}

# 2-d array 100x100 elements (int32, double)
rm -f /tmp/Array.data.100_100
../data_gen/gen_matrix -d 10 10 10 10 1.0 NG > /tmp/Array.data.100_100

for i in `seq 1 1`; do 
echo "Iteration $i..."

tiq "remove(Test_100_100)"
tiq "remove(Test_100_100_sg)"
tiq "CREATE ARRAY Test_100_100 < Num_One: int32, Num_Two: double > [ I=0:99,10,0, J=0:99,10,0 ]"
tiq "load (Test_100_100, '/tmp/Array.data.100_100')"
tiq "count(scan(Test_100_100))"
tiq "sg(scan(Test_100_100), Test_100_100_sg, 1)"
tiq "count(scan(Test_100_100_sg))"
ls -lh /tmp/Array.data.100_100
ls -lh ~/src/trunk/bin/storage.data1; pdsh -w instance2 "ls -lh bin/storage.data1"
exit

rm -f /tmp/Array.data.1000_100
../data_gen/gen_matrix -d 100 10 10 10 1.0 NG > /tmp/Array.data.1000_100

rm -f /tmp/Array.data.1000_1000
../data_gen/gen_matrix -d 10 10 100 100 1.0 NG > /tmp/Array.data.1000_1000

rm -f /tmp/Array.data.10000_10000
../data_gen/gen_matrix -d 100 100 100 100 1.0 NG > /tmp/Array.data.10000_10000

rm -f /tmp/Array.data.20000_20000
../data_gen/gen_matrix -d 200 200 100 100 1.0 NG > /tmp/Array.data.20000_20000

rm -f /tmp/Array.data.30000_30000
../data_gen/gen_matrix -r 30 30 1000 1000 1.0 NG > /tmp/Array.data.30000_30000


# 2-d array 1000x100 elements (int32, double) 
tiq "remove(Test_1000_100)"
tiq "remove(Test_1000_100_sg)"
tiq "CREATE ARRAY Test_1000_100 < Num_One: int32, Num_Two: double > [ I=0:999,10,0, J=0:99,10,0 ]"
tiq "load (Test_1000_100, '/tmp/Array.data.1000_100')" 
tiq "count(scan(Test_1000_100))"
tiq "sg(scan(Test_1000_100), Test_1000_100_sg, 1)" 
tiq "count(scan(Test_1000_100_sg))"
ls -lh /tmp/Array.data.1000_100 
ls -lh ~/src/trunk/bin/storage.data1; pdsh -w instance2 "ls -lh bin/storage.data1"

# 2-d array 1000x1000 elements (int32, double) 

tiq "remove(Test_1000_1000)"
tiq "remove(Test_1000_1000_sg)"
tiq "CREATE ARRAY Test_1000_1000 < Num_One: int32, Num_Two: double > [ I=0:999,100,0, J=0:999,100,0 ]"
tiq "load (Test_1000_1000, '/tmp/Array.data.1000_1000')" 
tiq "count(scan(Test_1000_1000))"
tiq "sg(scan(Test_1000_1000), Test_1000_1000_sg, 1)"
tiq "count(scan(Test_1000_1000_sg))" 
ls -lh /tmp/Array.data.1000_1000
ls -lh ~/src/trunk/bin/storage.data1; pdsh -w instance2 "ls -lh bin/storage.data1"


# 2-d array 10000x10000 elements (int32, double) 
tiq "remove(Test_10000_10000)"
tiq "remove(Test_10000_10000_sg)"
tiq "CREATE ARRAY Test_10000_10000 < Num_One: int32, Num_Two: double > [ I=0:9999,100,0, J=0:9999,100,0 ]"
tiq "load (Test_10000_10000, '/tmp/Array.data.10000_10000')" 
tiq "count(scan(Test_10000_10000))"
tiq "sg(scan(Test_10000_10000), Test_10000_10000_sg, 1)" 
tiq "count(scan(Test_10000_10000_sg))"
ls -lh /tmp/Array.data.10000_10000
ls -lh ~/src/trunk/bin/storage.data1; pdsh -w instance2 "ls -lh bin/storage.data1"


tiq "remove('Test_20000_20000)"
tiq "remove(Test_20000_20000_sg)"
tiq "CREATE ARRAY Test_20000_20000 < Num_One: int32, Num_Two: double > [ I=0:19999,100,0, J=0:19999,100,0 ]"
tiq "load (Test_20000_20000, '/tmp/Array.data.20000_20000')" 
tiq "count(scan(Test_20000_20000))"
tiq "sg(scan(Test_20000_20000), Test_20000_20000_sg, 1)" 
tiq "count(scan(Test_20000_20000_sg))"
ls -lh /tmp/Array.data.20000_20000
ls -lh ~/src/trunk/bin/storage.data1; pdsh -w instance2 "ls -lh bin/storage.data1"


tiq "remove(Test_30000_30000)"
tiq "remove(Test_30000_30000_sg)"
tiq "CREATE ARRAY Test_30000_30000 < Num_One: int32, Num_Two: double > [ I=0:29999,1000,0, J=0:29999,1000,0 ]"
tiq "load (Test_30000_30000, '/tmp/Array.data.30000_30000')" 
tiq "count(scan(Test_30000_30000))"
tiq "sg(scan(Test_30000_30000), Test_30000_30000_sg, 1)" 
tiq "count(scan(Test_30000_30000_sg))"
ls -lh /tmp/Array.data.30000_30000
ls -lh ~/src/trunk/bin/storage.data1; pdsh -w instance2 "ls -lh bin/storage.data1"

done 

# Run in-memory scatter gather tests
#for i in `seq 1 10`; do 
#    tiq "sg(scan(Test_100_100), )"
#    tiq "sg(scan(Test_1000_100), )"
#    tiq "sg(scan(Test_1000_1000), )"
#    tiq "sg(scan(Test_10000_10000), )"
#done
