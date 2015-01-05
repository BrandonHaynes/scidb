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
#  PGB: This description isn't correct. In addition to what I show here, I need 
#           to calculate the Sum ( Xi * 1.0 ) in addition to the Sum ( Xi * Xi ). This 
#           means augmenting the values with a row of "1.0" values. 
#
#  PGB: Also note - the numerical stability of these calculations is a bit suspect. 
#           I think we can fix this. 
#
#  This script is meant to show off the utility of SciDB's multiply 
# operation. The goal is to compute a variance/covariance matrix.
# 
#  Given a matrix Xnm, the variance/covariance matrix Sx is computed as: 
#   
#    Sx = 1/(n-1) * transpose ( X ) X
#
#  If you pick two columns of X say [i], and [j], then Sxij is the 
# covariance of X[*,i] and X[*,j]. If i = j, then you have the variance of 
# the values at X[*,i] in Sxii. 
#
#
# Environment variables. 
SCIDB_HOME=~/src/trunk/
SCIDB_BIN=$SCIDB_HOME/bin
DATA_HOME=/tmp
DATA_FILE=$DATA_HOME/Data
#
#
NUM_SYMS=9
NUM_TIMES=9999
#
# Phase 1: Set up. 
#
QN1="list ('arrays')"
QN2="remove (TS_Data)"
QN3="remove (TS_Datac)"
QN4="remove (TS_Skel_10)"
QN5="remove (TS_Skel_100)"
QN6="remove (TS_Manip_10)"
QN7="remove (TS_Manip_100)"
# QN3="CREATE ARRAY TS_Data < A1:int32, B1:double > [ I=0:99,100,0, J=0:99999,1000,0 ]"
QN8="CREATE ARRAY TS_Data < A1:int32, B1:double > [ I=0:99999,1000,0, J=0:99,100,0 ]"
QN9="load (TS_Data, '$DATA_FILE')"
#
QN10="CREATE ARRAY TS_Skel_10 < B1:double > [ I=0:0,100,0, J=0:9,100,0 ]"
QN11="CREATE ARRAY TS_Skel_100 < B1:double > [ I=0:0,100,0, J=0:99,100,0 ]"
#
QN12="store ( build ( TS_Skel_10, 0, 1 ), TS_Manip_10)"
QN13="store ( build ( TS_Skel_100, 0, 1 ), TS_Manip_100)"
#
#
# Create the "TS_Data data" load file. 
#
echo "#!/bin/sh"
echo "# "
echo "#  --== PHASE 1. Setup ==--"
echo "# "
echo "# Data Generation"
echo "# "
echo "$SCIDB_HOME/tests/data_gen/gen_matrix -r 100 1 1000 100 1.0 NG > $DATA_FILE"

QNMax=13;
QNNum=1;

while [ $QNNum -le $QNMax ]
do
    eval Query=\$QN$QNNum

    if [ "$Query" ]
	then
	echo "# "
	echo "#   Query = \"$Query\""
	echo "# "
	echo "$SCIDB_BIN/iquery --afl -q \"$Query\" > /dev/null"
	fi
    
    QNNum=`expr $QNNum + 1`;
done

#
# Phase 2: The queries. What does the input data look like?
#
#SELECT COUNT(*) FROM TS_Data; 
#
QS1="count ( TS_Data )"
#
#   SELECT * FROM TS_Data WHERE I BETWEEN 0 AND 10 AND J BETWEEN 0 AND 100;
QS2="project ( subarray ( TS_Data, 0, 0, $NUM_SYMS, $NUM_TIMES ), B1)"
#
#   WITH ( ) AS D,
#   SELECT * FROM transpose(D) MULTIPLY D;
# 
#   The following query generates a variance/covariance matrix. The input
#   is a sub-sample from the larger array -- only 10 rows, and only 10,000
#   values. 
#
QS3="store ( multiply ( 
	transpose ( project ( subarray ( TS_Data, 0, 0, $NUM_TIMES, $NUM_SYMS ), B1)),
	project ( subarray ( TS_Data, 0, 0, $NUM_TIMES, $NUM_SYMS ), B1)
), TS_Datac)"
#
#
QS4="scan ( TS_Datac )"
#
QS5="dimensions ( TS_Datac )"
QS6="attributes ( TS_Datac )"
# 
QS7="filter ( TS_Datac, J = J2 )"
#
QS8="project ( filter ( TS_Datac, J = J2 ), multiply)"
#
QS9="dimensions ( TS_Manip_10 )"
QS10="dimensions ( TS_Datac )"
#
#   Having computed the variance/covariance array, the next step is to 
#   pull out the variance statistics, which are found along the 
#   diagonal. This actually produces the standard deviation values. 
# 
#QS11="transpose ( 
#    project ( 
#	apply ( 
#	    multiply ( 
#		TS_Manip_10,
#		project ( filter ( TS_Datac, J = J2 ), multiply)
#		),
#	    stderr,
#	    sqrt(multiply / $NUM_TIMES.0 )
#	    ),
#	stderr
#	)
#)"
#
#  I can also compute the correlations, but converting all of the values 
#  in A[*,i] to a Z score according to the standard deviations computed
#  above, and then do the correlation calculation again. 

QS11="store(multiply(TS_Manip_10, TS_Datac), TS_Product)"
QS12="sum(TS_Product)"

QSMax=12;
QNNum=1;

while [ $QNNum -le $QSMax ]
do
    echo "# "
    eval Query=\$QS$QNNum
    if [ "$Query" ]
	then
	echo "#  "
	echo "echo Query = \"$Query\" "
	echo "#  "
	echo $SCIDB_BIN/iquery --afl -q \"$Query\"
	fi 
    QNNum=`expr $QNNum + 1`;
done
#
echo "# "

