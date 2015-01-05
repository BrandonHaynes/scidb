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
XSTARTS=(503000 504000 500000 504000 504000)
YSTARTS=(503000 491000 504000 501000 493000)
U1=9
U2=50
window="12,12"
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

gen(){
 # Data Generation
 ssdbgen -o -s -c tiny $SSDB/bin/tileData
 mv bench bench.pos $SSDB/data/tiny
}
init(){
 # Load the special libraries
 echo "Loading libs"
 iquery -naq "load_library('findstars')"
 iquery -naq "load_library('groupstars')"
 
 # Create Data Set tiny
 echo "Create Tiny Array"
 iquery -aq "remove (tiny)"
 iquery -aq "remove (tiny_space)"
 iquery -r /dev/null -aq "CREATE IMMUTABLE ARRAY tiny <a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32, j:int32, k:int32>[Z=0:19,1,0 ,J=0:9,10,0, I=0:9,10,0]"
 iquery -r /dev/null -aq "CREATE IMMUTABLE ARRAY tiny_space <I:int32, J:int32, index:int32>[id=0:19,20,0]"
 
 # Load Data tiny
 echo "Loading Tiny data .."
 START=$(date +%s)
 iquery -r /dev/null -aq "load(tiny, '$SSDB/data/tiny/bench')"
 iquery -r /dev/null -aq "load(tiny_space, '$SSDB/data/tiny/bench.pos')"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Loading Time: $DIFF seconds"

 # Cook Data tiny
 echo "Cooking Tiny data into tiny_obs array .."
 START=$(date +%s)
 iquery -r /dev/null -aq "store(findstars(tiny,a,450),tiny_obs)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Cooking Time: $DIFF seconds"

 # Group Data Normal 
 echo "Grouping Tiny observations into tiny_groups array .."
 START=$(date +%s)
 iquery -naq "store(groupstars(filter(tiny_obs,oid is not null and center is not null),tiny_space,0.1,20),tiny_groups)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Grouping Time: $DIFF seconds"
 
 # Split the observation
 echo "Pre-Observation spliting"
 python scripts/split_tiny.py

 # Redimension the Group array and precompute the groups centers
 iquery -naq "remove (tiny_groups_dim)"
 iquery -naq "remove (tiny_groups_centers)"
 iquery -naq "create array tiny_groups_dim<x:int64 NULL,y:int64 NULL> [group=0:*,1000,0,oid=0:*,1000,0,observation=0:*,20,0]"
 iquery -naq "store(redimension(tiny_groups,tiny_groups_dim),tiny_groups_dim)"
 iquery -naq "store(aggregate(tiny_groups,avg(x),avg(y),group),tiny_groups_centers)"
}

q1(){
 START=$(date +%s)
 iquery -r /dev/null -aq "aggregate(subarray(tiny,0,0,0,19,$U1,$U1),avg(a))"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q1: $DIFF seconds"
}

q2(){
 START=$(date +%s)
 iquery -r /dev/null -aq "findstars(subarray(tiny,0,0,0,0,$U1,$U1),a,450)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q2: $DIFF seconds"
}

q3(){
 START=$(date +%s)
 ##iquery -r /dev/null -aq "thin(window(subarray(tiny_reparted,0,0,0,19,$U1,$U1),1,4,4,avg(a)),0,1,2,3,2,3)"
 iquery -r /dev/null -aq "aggregate(thin(window(repart(subarray(project(tiny,a),0,0,0,19,$U1,$U1),<a:int32>[Z=0:19,1,0,J=0:9,12,0,I=0:9,12,0]),1,4,4,avg(a)),0,1,2,3,2,3),avg(a_avg))"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q3: $DIFF seconds"
}

q4(){
 START=$(date +%s)
 for (( i=0; i < 20 ; i++ )) do
   iquery -r /dev/null -aq  "aggregate(filter(subarray(tiny_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),center is not null),avg(sumPixel))" 
 done
 wait
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q4: $DIFF seconds"
}

q5(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -aq  "filter(subarray(tiny_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),polygon is not null)" 
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q5: $DIFF seconds"
}

q6(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -o csv+ -r /dev/null -aq  "filter(window(filter(subarray(tiny_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),center is not null),$window,$window,count(center)),center_count>$count)"
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q6: $DIFF seconds"
}

q7(){
  START=$(date +%s)
  iquery -o csv+ -r /dev/null -aq  "filter(aggregate(tiny_groups,avg(x),avg(y),group),x_avg > ${XSTARTS[$ind]} and y_avg > ${YSTARTS[$ind]} and x_avg < ${XSTARTS[$ind]}+$U2 and y_avg < ${YSTARTS[$ind]}+$U2)"
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
  iquery -naq "remove(Points)" > /dev/null
  iquery -naq "create array Points<ID:int64,x:int64,y:int64>[INDEX=0:3,4,0]" > /dev/null
  iquery -naq "load(Points,'/tmp/Points.dat')" > /dev/null


  START=$(date +%s)
  iquery -r /dev/null -o csv+ -aq  "
       aggregate(
          cross_join(tiny_groups,
          filter(
                aggregate ( 
                        project ( 
                                apply ( 
                                        cross_join ( 
                                                subarray ( Points, 0,3 ),
                                                join ( 
                                                        subarray (tiny_groups, NULL,NULL,NULL,18) AS Pi,
                                                        subarray (tiny_groups, NULL,1,NULL,NULL) AS Pj
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
        avg(tiny_groups.x),avg(tiny_groups.y),tiny_groups.group)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q8: $DIFF seconds"
}

q9(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
   iquery -r /dev/null -o csv+ -aq  "cross_join(cross_join(tiny_groups_dim, store(redimension(project(apply(filter(between(tiny_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),polygon is not null),check,true),oid,check), b), b) as b, tiny_groups_dim.oid,b.oid) as A,tiny_groups_centers as C,A.group,C.group)"
  done
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q9: $DIFF seconds"
}

echo "SSDB:"
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
## ADD more repetitions here
for rep in 0 #1 2
do
 for ind in 0 1 2 3 4
 do
  echo "Run [$(($rep*5+$ind))]:"
  q1 
  q2
  q3
  q4
  q5
  q6
  q7
  # TODO: This part should be done in Python
  #q8
  #q9
 done
done

echo "[End]"
