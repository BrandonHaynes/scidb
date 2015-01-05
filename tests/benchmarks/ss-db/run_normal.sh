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
U1=7499
U2=9999
U3=999
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
 ssdbgen -o -s -c normal $SSDB/bin/tileData
 mv bench bench.pos $SSDB/data/normal
}
init(){
 # Load the special libraries
 echo "Loading libs"
 iquery -naq "load_library('findstars')"
 iquery -naq "load_library('groupstars')"
 
 # Create Data Set Normal 
 echo "Create Normal Array"
 #iquery -naq "CREATE IMMUTABLE ARRAY normal <a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32, j:int32, k:int32>[Z=0:399,1,0 ,J=0:7499,7500,0, I=0:7499,7500,0]"
 #iquery -naq "CREATE IMMUTABLE ARRAY normal_space <I:int32, J:int32, index:int32>[id=0:399,400,0]"
 
 # Load Data Normal 
 echo "Loading Normal data .."
 START=$(date +%s)
 ## Parallel version
 #iquery -naq "load(normal, '/data/normal/bench',-1)"
 #iquery -naq "load(normal_space, '/data/normal/bench.pos')"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Loading Time: $DIFF seconds"
 
 # Cook Data Normal 
 echo "Cooking Normal data into normal_obs array .."
 START=$(date +%s)
 iquery -naq "store(findstars(normal,a,900),normal_obs)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Cooking Time: $DIFF seconds"
 
 # Group Data Normal 
 echo "Grouping Normal observations into normal_groups array .."
 START=$(date +%s)
 iquery -naq "store(groupstars(filter(normal_obs,oid is not null and center is not null),normal_space,0.1,20),normal_groups)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Grouping Time: $DIFF seconds"
 

 #pre-reparting the Main array 
 #NOTE: This is commented out because inlining the repart directly in Q3 is better way to go, though it's slower.
 #echo "Pre-Reparting the Main Array"
 #iquery -naq "store(repart(subarray(normal,0,0,0,19,$U1,$U1),<a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32,j:int32, k:int32>[Z=0:19,1,0,J=0:7499,7500,2,I=0:7499,7500,2]),normal_reparted)"
  
 # Split the observations
 echo "Pre-Observation spliting"
 #python scripts/split_normal.py
 
 # Redimension the Group array and precompute the groups centers
 iquery -naq "create array temp<check:bool NULL>[oid=0:*,1000,0]"
 iquery -naq "create array normal_groups_dim<x:int64 NULL,y:int64 NULL> [group=0:*,1000,0,oid=0:*,1000,0,observation=0:*,20,0]"
 iquery -naq "store(redimension(normal_groups,normal_groups_dim),normal_groups_dim)"
 iquery -naq "store(aggregate(normal_groups,avg(x),avg(y),group),normal_groups_centers)"
}

q1(){
 START=$(date +%s)
 iquery -r /dev/null -aq "aggregate(between(normal,0,0,0,19,$U1,$U1),avg(a))"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q1: $DIFF seconds"
}

q2(){
 START=$(date +%s)
 iquery -r /dev/null -aq "findstars(between(normal,0,0,0,0,$U1,$U1),a,900)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q2: $DIFF seconds"
}

q3(){
 START=$(date +%s)
 #iquery -r /dev/null -aq "aggregate(thin(window(between(project(normal_reparted,a), 0,0,0,19,$U1,$U1),1,4,4,avg(a)),0,1,2,3,2,3),avg(a_avg))"
 iquery -r /dev/null -aq "thin(window(repart(subarray(project(normal,a),0,0,0,19,7499,7499),<a:int32>[Z=0:19,1,0,J=0:7499,7500,2,I=0:7499,7500,2]),1,4,4,avg(a)),0,1,2,3,2,3)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q3: $DIFF seconds"
}

q4(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -o csv+ -aq  "aggregate(filter(between(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),center is not null),avg(sumPixel))" 
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q4: $DIFF seconds"
}

q5(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -o csv+ -aq  "filter(between(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),polygon is not null)" 
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q5: $DIFF seconds"
}

q6(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -o csv+ -aq  "filter(window(filter(between(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),center is not null),$window,$window,count(center)),center_count>$count)"
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q6: $DIFF seconds"
}

q7(){
  START=$(date +%s)
  iquery -o csv+ -r /dev/null -aq  "filter(aggregate(normal_groups,avg(x),avg(y),group),x_avg > ${XSTARTS[$ind]} and y_avg > ${YSTARTS[$ind]} and x_avg < ${XSTARTS[$ind]}+$U2 and y_avg < ${YSTARTS[$ind]}+$U2)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q7: $DIFF seconds"
}

q8(){

  echo "[
(0,${XSTARTS[$ind]},${YSTARTS[$ind]}),
(1,$[${XSTARTS[$ind]}+$U3],${YSTARTS[$ind]}),
(2,$[${XSTARTS[$ind]}+$U3],$[${YSTARTS[$ind]}+$U3]),
(3,${XSTARTS[$ind]},$[${YSTARTS[$ind]}+$U3])
]" > /tmp/Points.dat
  iquery -naq "remove(Points)"
  iquery -naq "create array Points<ID:int64,x:int64,y:int64>[INDEX=0:3,4,0]"
  iquery -naq "load(Points,'/tmp/Points.dat')"

  START=$(date +%s)
  python scripts/q8.py
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q8: $DIFF seconds"
}

q9(){
  START=$(date +%s)
  #for (( i=0; i < 20 ; i++ )) do
   #iquery -r /dev/null -o csv+ -aq  "cross_join(cross_join(normal_groups_dim, store(redimension(project(apply(filter(between(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U3,${YSTARTS[$ind]}+$U3),polygon is not null),check,true),oid,check), temp), temp) as b, normal_groups_dim.oid,b.oid) as A,normal_groups_centers as C,A.group,C.group)"
  #done
  python scripts/q9.py $ind
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q9: $DIFF seconds"
}


echo "** SSDB BENCHMARK [normal configuration] *****************"

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
