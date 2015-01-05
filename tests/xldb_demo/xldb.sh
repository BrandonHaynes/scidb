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
#  This script is intended to show off the SciDB engine at the XLDB 2010 
#  meeting at SLAC in California. We will focus here on the DML workload,
#  rather than the entire CREATE / LOAD stuff, mostly because load takes 
#  a long time to do. 
#
# Environment variables. 
SCIDB_HOME=~/src/trunk
SCIDB_BIN=$SCIDB_HOME/bin
DATA_HOME=/tmp
DATA_FILE=$DATA_HOME/Data
#
# Phase 1: Set up. 
#
QN1="list ('arrays')"
QN2="remove (Big)"
QN3="CREATE ARRAY Big < A1:int32, B1:double, C1:char > [ I=0:9999,1000,0, J=0:9999,1000,0 ]"
QN4="load (Big, '$DATA_FILE')"
#
# Create the "big data" load file. 
#
echo "#!/bin/sh"
echo "# "
echo "#  --== PHASE 1. Setup ==--"
echo "# "
echo "# Data Generation"
echo "# "
echo "$SCIDB_HOME/tests/data_gen/gen_matrix -r 10 10 1000 1000 1.0 NGC > $DATA_FILE"

QNMax=4;
QNNum=1;

while [ $QNNum -le $QNMax ]
do
	echo "# "
	eval Query=\$QN$QNNum
	echo "#   Query = $Query"
	echo "# "
	echo "$SCIDB_BIN/iquery --afl  -n -q \"$Query\" \> /dev/null"
	
	QNNum=`expr $QNNum + 1`;
done
#
# Phase 2: The queries. 
#
#	SELECT COUNT(*) FROM Big; 
#
QS1="count ( Big )"
#
#   SELECT * FROM Big A WHERE A.I BETWEEN :I1 AND :I2 AND A.J BETWEEN :J1 AND :J2;
#
QS2="subarray ( Big, 5000, 5000, 5010, 5010 )"
#
#   SELECT * FROM Big A 
#    WHERE A.I BETWEEN :I1 AND :I2 AND A.J BETWEEN :J1 AND :J2 AND A.C1 = char('X');
#
QS3="filter ( subarray ( Big, 5000, 5000, 5010, 5010 ), C1 = char('X'))"
#
#    SELECT COUNT(*) FROM Big 
#     WHERE A.I BETWEEN :I1 AND :I2 AND A.J BETWEEN :J1 AND :J2
#    GROUP BY A.I;
#
QS4="count ( subarray ( Big, 5000, 5000, 5010, 5010 ), I)"
#
#    SELECT COUNT(*) FROM Big 
#     WHERE A.I BETWEEN :I1 AND :I2 AND A.J BETWEEN :J1 AND :J2
#       AND C1 = char('X')
#    GROUP BY A.I;
#
QS5="count ( filter ( subarray ( Big, 5000, 5000, 5010, 5010 ), C1 = char('X')), I)"
#
#  Bigger box? 
#
QS6="count ( filter ( subarray ( Big, 5000, 5000, 5100, 5100 ), C1 = char('X')), I)"
#
#
QS7="
project ( 
  apply ( 
	join ( 
		subarray ( Big , 5000, 5000, 5010, 5010 ) AS Ar1,
		subarray ( Big , 4999, 4999, 5009, 5009 ) AS Ar2
	),
	Diff,
	Ar1.B1 - Ar2.B1
  ), Diff
)"
# SELECT Ar.B1 - Ar2.B1 AS Diff
# FROM Big AS Ar1, Big AS Ar2
# WHERE Ar1.I BETWEEN 5000 AND 5010 AND Ar1.J BETWEEN 5000 AND 5001
#   AND Ar2.I BETWEEN 4999 AND 5009 AND Ar2.J BETWEEN 4999 AND 5009
#
#
QS8="
count ( 
	filter ( 
		project ( 
  			apply ( 
				join ( 
					subarray ( Big , 5000, 5000, 5010, 5010 ) AS Ar1,
					subarray ( Big , 4999, 4999, 5009, 5009 ) AS Ar2
				),
			Diff,
			Ar1.B1 - Ar2.B1
  			), 
		Diff
		),
	Diff > 0
	)
)"
# SELECT * FROM
# ( SELECT Ar.B1 - Ar2.B1 AS Diff
#   FROM Big AS Ar1, Big AS Ar2
#   WHERE Ar1.I BETWEEN 5000 AND 5010 AND Ar1.J BETWEEN 5000 AND 5001
#   AND Ar2.I BETWEEN 4999 AND 5009 AND Ar2.J BETWEEN 4999 AND 5009 )
# WHERE Diff > 0
#
QS9="
apply ( 
	join ( 
		count ( 
			filter ( 
				project ( 
  					apply ( 
						join ( 
							subarray ( Big , 5000, 5000, 5500, 5500 ) AS Ar1,
							subarray ( Big , 5000, 4999, 5500, 5499 ) AS Ar2
						),
					Diff,
					Ar1.B1 - Ar2.B1
  					), 
				Diff
				),
				Diff > 0
			)
		) AS N,
   	count ( 
        	project (  
            	apply ( 
                	join ( 
                    	subarray ( Big , 5000, 5000, 5500, 5500 ) AS Ar1,
                    	subarray ( Big , 5000, 4999, 5500, 5499 ) AS Ar2
                	),
            	Diff,
            	Ar1.B1 - Ar2.B1
            	), 
        	Diff
        	)
    	) AS D
	),
	P,
	abs(N.count) / abs(D.count)
)"
#
# select abs(N)/abs(D)
# from
# count(
#  select *
#  from Big as Ar1, Big as Ar2
#  where  A.i between 5000 and 5500 and A.j between 4999 and 5499
#             B.i between 5000 and 5500 and Bj between 5000 and 5500
#  and Ar1.B1 - Ar2.B2 > 0 ) as N,
#
# count(
#   select *
#   from Big as Ar1, Big as Ar2
#   where  A.i between 5000 and 5500 and A.j between 4999 and 5499
#            B.i between 5000 and 5500 and Bj between 5000 and 5500 ))
#     as D
#
#
QS10="
apply ( 
    join ( 
        count ( 
            filter ( 
                project ( 
					filter (
                    	apply ( 
                        	join ( 
                            	subarray ( Big , 5000, 5000, 5500, 5500 ) AS Ar1,
                            	subarray ( Big , 5000, 4999, 5500, 5499 ) AS Ar2
                        	),
                    	Diff,
                    	Ar1.B1 - Ar2.B1
                    	), 
             		Ar1.C1 = Ar2.C1 
					)
                Diff
                ),
                Diff > 0
            )
        ) AS N,
    	count ( 
        	project (  
		    	filter ( 
                	apply ( 
                   		join ( 
                       		subarray ( Big , 5000, 5000, 5500, 5500 ) AS Ar3,
                       		subarray ( Big , 5000, 4999, 5500, 5499 ) AS Ar4
                   		),
               		Diff,
               		Ar3.B1 - Ar4.B1
               		), 
             	Ar3.C1 = Ar4.C1 
				),
        	Diff
			),
    	) AS D
    ),
    P,
    abs(N.count) / abs(D.count)
)"


QSMax=9;
QNNum=1;

while [ $QNNum -le $QSMax ]
do
    echo "# "
    eval Query=\$QS$QNNum
    echo "#   Query = $Query"
    echo "# "
    echo $SCIDB_BIN/iquery --afl  -q \"$Query\"
    
    QNNum=`expr $QNNum + 1`;
done

