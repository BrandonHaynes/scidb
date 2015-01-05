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
#   File:  Arrays.sh
# 
#   About: 
#
#   This script takes 5 arguments: 
#
#   Arrays.sh Chunk_Count Chunk_Length Zipf Sparsity Instance
#
#
echo ""
echo "+================================================"
echo "||                                             ||"
echo "||                  Arrays.sh                  ||"
echo "|| DENSE Array, UNIFORM Distribution of Values ||"
echo "|| Chunk Count = $1, Chunk Length = $2 ||"
echo "||       Sparsity = $3,   Zipf = $4    ||"
echo "||        Instance_LABEL = $5       ||"
echo "||                                             ||"
echo "+================================================"
echo ""
date
echo ""
#  
# set -x
#
LEN_I=`expr $1 "*" $2 - 1`;
LEN_J=`expr $1 "*" $2 - 1`;
ZIPF=$3;
SPRS=$4;
INST=$5;
Port=$6;
LABEL="Array_${ZIPF}_${SPRS}";
#
#  ADMIN 1: Nuke the previous instance to clean up the space. 
yes | scidb.py initall $INST
scidb.py startall $INST
#
sleep 3;
#
#  DDL 1: Create array with 5 attributes for loading. 
CMD="CREATE ARRAY Test_Array_Raw <
    I           : int64,
    J           : int64,
    int_attr_1  : int64,
    int_attr_2  : int64,
    double_attr : double
>
[ RowNum=0:*,1000000,0 ]"
#
echo "${CMD}"
time -p iquery --port $Port -aq "$CMD"
#
#  DDL 2: Create array with 3 attributes
#
CMD="CREATE ARRAY Test_Array <
    int_attr_1  : int64,
    int_attr_2  : int64,
    double_attr : double
>
[ I=0:$LEN_I,$2,0, J=0:$LEN_J,$2,0 ]"
#
echo "${CMD}"
time -p iquery --port $Port -aq "$CMD"
#
#    Populating this array using the build() is problematic, as here are three 
#  attributes and the distributions are awkward. So instead I will use the 
#  external gen_matrix executable to generate the data, and load it using a
#  pipe.
#
OUT_FILE="/public/users/plumber/Scale_Test_Data/Test_Array_Data_${1}_${2}_${ZIPF}_${SPRS}.scidb"
#
rm -rf /tmp/Load.pipe
mkfifo /tmp/Load.pipe
echo "gen_matrix -rb $1 $1 $2 $2 $ZIPF $SPRS NNG > /tmp/Load.pipe"
gen_matrix -rb $1 $1 $2 $2 $ZIPF $SPRS NNG > /tmp/Load.pipe &
#
#  Time the load. 
/usr/bin/time -f "QLoad $LABEL Load %e" iquery --port $Port -nq "load Test_Array_Raw FROM '/tmp/Load.pipe' AS '(INT64,INT64,INT64,INT64,DOUBLE)';"
#
#  DML 1: How many cells in this array?
CMD="join ( 
        build ( < s : string > [ I=0:0,1,0 ], 'Size_Count_For ${LABEL}'),
        aggregate ( Test_Array_Raw, count(*) )
)";
#
echo $CMD
#
/usr/bin/time -f "${LABEL} Cell_Count %e" iquery --port $Port -taq "$CMD"
# 
#  Find out how big storage.data1 on the 0 instance is. 
#
du -b $SCIDB_DATA_DIR/000/0/storage.data1
#
# save() the array, and compress it. 
#
CMD="
save ( Test_Array_Raw, '${OUT_FILE}', 0, '(INT64,INT64,INT64,INT64,DOUBLE)')
"
echo "${CMD}"
#
date;
# /usr/bin/time -f "QSave %e" iquery --port $Port -r /dev/null -naq "${CMD}"
ps -eo comm,%mem | grep SciDB-000-0
#
# Compress it. 
# gzip ${OUT_FILE}
#
# END 
