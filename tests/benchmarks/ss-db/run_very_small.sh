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

### Input parameters
YSTARTS=(503000 504000 500000 504000 504000)
XSTARTS=(503000 491000 504000 501000 493000)
U1=1599
U2=2500
window="7,7"
count=1
### SSDB directory
THIS=`pwd`
while [ -h "$THIS" ]; do
  ls=`ls -ld "$THIS"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    THIS="$link"
  else
    THIS=`dirname "$THIS"`/"$link"
  fi
done
SSDB="$THIS"
###
SSDB=''
gen(){
 # Data Generation
 ssdbgen -o -s -c very_small ./bin/tileData
 mv bench bench.pos $SSDB/data/very_small
}
init(){
 # Load the Findstars library
 echo "Loading Findstars"
 iquery -r /dev/null -aq "load_library('findstars')"
 
 # Create Data Set Very_Small 
 echo "Create Very_Small Array"
 iquery -naq "remove(very_small)"
 iquery -naq "remove(very_small_space)"
 iquery -naq "CREATE IMMUTABLE ARRAY very_small <a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32, j:int32, k:int32>[Z=0:39,1,0 ,J=0:1599,1600,0, I=0:1599,1600,0]"
 iquery -naq "CREATE IMMUTABLE ARRAY very_small_space <I:int32, J:int32, index:int32>[id=0:39,40,0]"
 
 # Load Data Very_Small 
 echo "Loading Very_Small data .."
 START=$(date +%s)
 iquery -naq "load(very_small, '$SSDB/data/very_small/bench')"
 iquery -naq "load(very_small_space, '$SSDB/data/very_small/bench.pos')"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Loading Time: $DIFF seconds"
 
 # Cook Data Very_Small 
 echo "Cooking Very Small data into very_small_obs array .."
 START=$(date +%s)
 iquery -r /dev/null -aq "store(findstars(very_small,a,900),very_small_obs)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Cooking Time: $DIFF seconds"

 # Group Data Small 
 echo "Grouping Very_Small observations into very_small_groups array .."
 START=$(date +%s)
 iquery -naq "store(groupstars(filter(very_small_obs,oid is not null and center is not null),very_small_space,0.025,20),very_small_groups)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Grouping Time: $DIFF seconds"
 
 # pre-reparting the array 
 echo "Pre-Reparting the Array"
 iquery -r /dev/null -aq "store(repart(subarray(very_small,0,0,0,19,1599,1599),<a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32,j:int32, k:int32>[Z=0:19,1,0,J=0:1599,1602,2,I=0:1599,1602,2]),very_small_reparted)"
 
 # Split the observations
 echo "Pre-Observation spliting"
 python scripts/split_very_small.py
}

q1(){
 START=$(date +%s)
 iquery -r /dev/null -aq "aggregate(subarray(very_small,0,0,0,19,1599,1599),avg(a))"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q1: $DIFF seconds"
}

q2(){
 START=$(date +%s)
 iquery -r /dev/null -aq "findstars(subarray(very_small,0,0,0,0,1599,1599),a,900)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q2: $DIFF seconds"
}

q3(){
 START=$(date +%s)
 iquery -r /dev/null -aq "thin(window(subarray(very_small_reparted,0,0,0,19,1599,1599),1,4,4,avg(a)),0,1,2,3,2,3)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q3: $DIFF seconds"
}

q4(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -aq  "aggregate(filter(subarray(very_small_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U1,${YSTARTS[$ind]}+$U1),center is not null),avg(sumPixel))" &
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q4: $DIFF seconds"
}

q5(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -aq  "filter(subarray(very_small_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U1,${YSTARTS[$ind]}+$U1),polygon is not null)" &
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q5: $DIFF seconds"
}

q6(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -o csv+ -r /dev/null -aq  "filter(window(filter(subarray(very_small_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U1,${YSTARTS[$ind]}+$U1),center is not null),$window,$window,count(center)),center_count>$count)"
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q6: $DIFF seconds"
}

q7(){
  START=$(date +%s)
  iquery -o csv+ -r /dev/null -aq  "filter(aggregate(very_small_groups,avg(x),avg(y),group),x_avg > ${XSTARTS[$ind]} and y_avg > ${YSTARTS[$ind]} and x_avg < ${XSTARTS[$ind]}+$U2 and y_avg < ${YSTARTS[$ind]}+$U2)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q7: $DIFF seconds"
}

q8(){

  echo "[
(0,${XSTARTS[$ind]},${YSTARTS[$ind]}),
(1,$[${XSTARTS[$ind]}+$U2],${YSTARTS[$ind]}),
(2,$[${XSTARTS[$ind]}+$U2],$[${YSTARTS[$ind]}+$U2]),
(3,${XSTARTS[$ind]},$[${YSTARTS[$ind]}+$U2])
]" > /tmp/Points.dat
  iquery -naq "remove(Points)"
  iquery -naq "create array Points<ID:int64,x:int64,y:int64>[INDEX=0:3,4,0]"
  iquery -naq "load(Points,'/tmp/Points.dat')"


  START=$(date +%s)
  iquery -r /dev/null -o csv+ -aq  "
       aggregate(
          cross_join(very_small_groups,
          filter(
                aggregate ( 
                        project ( 
                                apply ( 
                                        cross_join ( 
                                                subarray ( Points, 0,3 ),
                                                join ( 
                                                        subarray (very_small_groups, NULL,NULL,NULL,18) AS Pi,
                                                        subarray (very_small_groups, NULL,1,NULL,NULL) AS Pj
                                                )
                                        ),
                                        crosses,
                                        iif (((((Pi.y <= Points.y) AND (Pj.y > Points.y)) OR 
                                        ((Pi.y > Points.y) AND (Pj.y <= Points.y))) AND 
                                        (Points.x < Pi.x + ((Points.y - Pi.y) / (Pj.y - Pi.y)) * (Pj.x - Pi.x)) AND (Pi.x is not null and Pi.y is not null and Pj.x is not null and Pj.y is not null)),
                                        1, 0 )
                                ),
                                crosses
                        ),
                        sum(crosses),Pj.group
                ),
           crosses_sum > 0)
        ),
        avg(very_small_groups.x),avg(very_small_groups.y),very_small_groups.group)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q8: $DIFF seconds"
}

q9(){
  START=$(date +%s)
  iquery -r /dev/null -o csv+ -aq  "Aggregate(filter(filter(cross_join(subarray(very_small_obs_0,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2) as A, very_small_groups),A.polygon is not null), A.oid=very_small_groups.oid),avg(very_small_groups.x),avg(very_small_groups.y),very_small_groups.group)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q9: $DIFF seconds"
}

echo "** SSDB BENCHMARK [very small configuration] *****************"

if [[ $1 = "-g" ]]; then
 echo "[.... Data Generation ....]"
 gen 
 echo "[..... Initialization ....]"
 init
fi
if [[ $1 = "-i" ]]; then
 echo "[.... Initialization ....]"
 init
fi

echo "[Begin]"
## ADD repetition here if needed
for rep in 0 1 2
do
 for ind in 0 1 2 3 4
 do
  echo "run [$(($rep*5+$ind))]:"
  q1
  q2
  q3
  q4
  q5
  q6
  q7
  q8
  q9
 done
done

echo "[End]"
