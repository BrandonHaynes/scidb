#!/bin/sh -V
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
#  This script:
#    1. Generates a large number of arrays of various sizes and with a variety 
#       of attribute lists. 
#
#    2. For each array, generates external load files for both the dense, and 
#       sparse cases. 
#
#    3. Creates the array, and loads the generated data. 
#
#    4. Runs a set of queries over the arrays. These queries are designed in such 
#       a way that the answers to the queries--which typically combine several 
#       SciDB operators--can be determined based on the pattern of data in the 
#       load file. 
#
#    5. Checks that the answers to these queries are 
#       'correct' - or at least, consistent. 
#
#  This script only tests 2D arrays, but with a bit more work -- mainly changes 
#  to the way the gen_matrix program does its thing -- it could be made to work 
#  on arrays With Rank > 2. 
#
#  Environment Related Stuff: 
# 
#  GEN_DATA - this is the pointer to the executable that generates
#             the load files. 
#
GEN_DATA="~/src/b_0_5_release/tests/data_gen/gen_matrix"
#
#  attr - attribute signature. These strings are pairs, delimitered by a '?' 
#         character, that contain the AQL syntax preceeding the delim, and the 
#         corresponding gen_matrix 'string' command line argument following the 
#         delim. 
#
#  Notes on Bugs: 
#
# for attr in "A: int32?N" "A: int32, B: float?NG" 

for attr in "A: int32, B: string?NS" "A: double, B: int32?GN" "A: int32, B: float?NG"

do

	str=`expr "$attr" : '.*?\(.*\)$'`;
	hdr=`expr "$attr" : '\(.*\)?.*$'`;
#
#  NOTE: All of the values used to specificy chunk counts, and chunk sizes, 
#        are odd. This is an important detail, because it means that the 
#        'values' in the attributes will be an arithmetic progression starting 
#        at 0, and ending with the value (nCellMax - 1). The important point 
#        being that this set of data gives us a value 'in the middle' that 
#        can be figured out algebraicly, which it so happens will also equal 
#        the mean and median values. 
#
#   rc - Row Chunks. The number of chunks in the <- 'I' -> dimension.
#   for rc in 1 3 7 21 315
#   for rc in 1 7 63 

  	for rc in 1 3
 	do
#
#  cc - Column Chunks. The number of chunks in the v- 'J' -^ dimension. 
#  		for cc in 1 3 7 15 21 63 315
#  		for cc in 1 3 
#  		for cc in 1 7 63

   		for cc in 1 3 
 		do 
#
# rprc - Rows Per Row Chunk - the number of rows in each chunk in the 
#        <- 'I' -> dimension. 
#		    for rprc in 7 21 35 49 91 147 441 1323 3969 
#		    for rprc in 7 147 

  		    for rprc in 7
 			do 
#
# For efficiency, we'll try to calculate values as soon as we can. 
# 
#  r - Rows - the total number of rows in the target array. 
 				r=`expr $rc \* $rprc`;
#
#  mr - Maximum Row Index - as the dimension index values begin at 
#       0, and the CREATE ARRAY statement requires the maximum 
#       offset, we need to compute the value to use.
 				mr=`expr $r - 1`;
#
#
# cpcc - Columns Per Column Chunk - the number of columns in each 
#        chunk in the v- 'J' -^ dimension. 
#  				for cpcc in 7 21 35 49 91 147 441 1323 3969 
# 				for cpcc in 10
 		    	for cpcc in 7 147 
 				do 
# 
#  c - Columns - the number of columns in the target array. 
 					c=`expr $cc \* $cpcc`;
#
#  size - Cell Count - the total number of cells in the target 
#         array. 
 					size=`expr $r \* $c`;
#
#  m - Maximum Cell Index - 
 					m=`expr $size - 1`;
#
#  mc - Maximum Column Index - as the dimension index values begin 
#       at 0, and the CREATE ARRAY statement requires the maximum
#       offset, we need to compute the value to use here. 
 					mc=`expr $c - 1`;

#
#  aname - name of the array. The name is simply a conjunction
#          of the sizing information, and the idea is to keep 
#          the name unique. 

  					aname="T_"$str"_"$cc"_"$rc"_"$cpcc"_"$rprc;

#
#  mid - middle value of A: 
#					mid=`expr $size / 2`;

					mid=`expr \( $c / 2  \) + \( \( $r - 1 \) / 2 \) \* $c`;
#
# 
echo "# r = $r, c = $c, size = $size and mid = $mid"

#
#  The guts of the testing is a series of create / load / 
#  query ... query ... query / remove operations. These
#  have the form below. Ideally it would be nice to have another 
#  file to which queries might be added. 
#
#  Test Part 1: CREATE, LOAD and COUNT. 
CA="./iquery -q \"CREATE ARRAY $aname < $hdr > [ I=0:$mc,$cpcc,0, J=0:$mr,$rprc,0 ]\"";
	for P in "1.0" "0.01" 
	do 
GA="$GEN_DATA -d $cc $rc $cpcc $rprc $P $str > /tmp/Array.data"
LA="./iquery -q -n \"load ('$aname', '/tmp/Array.data')\""

 				MidI=`expr \( $size / 2 \) + 1`;

				MidX=`expr $c / 2`;
				MidY=`expr $r / 2`;

				WinX=`expr $cpcc / 3`;
				WinY=`expr $rprc / 3`;

 				MinX=`expr $MidX - $WinX`;
				MinY=`expr $MidY - $WinY`;

				MaxX=`expr $MidX + $WinX`;
				MaxY=`expr $MidY + $WinY`;

#
# How many cells? 
QS1="count ( $aname )"

#
# Pull a subarray()
QS2="subarray ( $aname, $MinX, $MinY, $MaxX, $MaxY )"
QS3="count ( $QS2 )"

#
# Run a couple of apply() operators over the subarray()
QS4="apply ( $QS2 AS Q, 'QR', Q.A + 2 )"

#
# Get some aggregates() 
QS5="count ( $QS4 )"
QS6="sum ( $QS4, 'QR' )"
QS7="average ( $QS4, 'QR' )"
QS8="min ( $QS4, 'QR' )"
QS9="max ( $QS4, 'QR' )"

# Another apply() - '-' 
QS10="apply ( $QS2 AS Q, 'QR', Q.A - 2 )"
QS11="count ( $QS10 )"
QS12="sum ( $QS10, 'QR' )"
QS13="average ( $QS10, 'QR' )"
QS14="min ( $QS10, 'QR' )"
QS15="max ( $QS10, 'QR' )"

#
# Yet another apply()  - '/' 
QS16="apply ( $QS2 AS Q, 'QR', Q.A / 2 )"
QS17="count ( $QS16 )"
QS18="sum ( $QS16, 'QR' )"
QS19="average ( $QS16, 'QR' )"
QS20="min ( $QS16, 'QR' )"
QS21="max ( $QS16, 'QR' )"

#
# Yet another apply()  - '*' 
QS22="apply ( $QS2 AS Q, 'QR', Q.A * 2 )"
QS23="count ( $QS22 )"
QS24="sum ( $QS22, 'QR' )"
QS25="average ( $QS22, 'QR' )"
QS26="min ( $QS22, 'QR' )"
QS27="max ( $QS22, 'QR' )"

# And now, some filters. 
QS28="filter ( $QS4, QR > $mid )"
QS29="count ( $QS28 )"
QS30="sum ( $QS28, 'QR' )"

QS31="filter ( $QS4, QR >= $mid )"
QS32="count ( $QS31 )"
QS33="sum ( $QS31, 'QR' )"

QS34="filter ( $QS4, QR = $mid )"
QS35="count ( $QS34 )"
QS36="sum ( $QS34, 'QR' )"

QS37="filter ( $QS4, QR <= $mid )"
QS38="count ( $QS37 )"
QS39="sum ( $QS37, 'QR' )"

QS40="filter ( $QS4, QR <= $mid )"
QS41="count ( $QS40 )"
QS42="sum ( $QS40, 'QR' )"

#
# Let's permute the order. Note that the filter() must be 
# outside the apply. 
#  BUG: Cannot permute order. 
# filter(apply(subarray()), filter(subarray(apply())),   
# subarray(filter(apply()))
#
# filter( apply ( $QS2 AS Q, 'APPLY', Q.A + 2 ), APPLY > $mid )
# filter( subarray ( apply ( $aname AS Q, 'APPLY', Q.A + 2) , $MinX, $MinY, $MaxX, $MaxY), APPLY > $mid )
# subarray ( filter ( apply ( $aname AS Q, 'APPLY', Q.A + 2 ), APPLY > $mid ) , $MinX, $MinY, $MaxX, $MaxY )
#
# 

MaxQNum=42 

DA="./iquery -q \"remove ( '$aname' )\""

if [ 1 = 1 ] 
then 

# echo "CA = "
echo "$CA"

# echo "GA = "
echo "$GA"

# echo "LA = "
echo "$LA"

#
# Loop over the queries. 
#
		Qnum=1;
		while [ $Qnum -le $MaxQNum ] 
		do 
			eval Query=\$QS$Qnum
			
			echo "echo \"Query = $Query \""
			echo "./iquery -q \"$Query\""
			echo "echo \"\""

			Qnum=`expr $Qnum + 1`;
		done 

# echo "DA = "
echo "$DA"

fi
					done 
 				done
 			done 
 		done
 	done
done
#
echo "./iquery -q \"list ('arrays')\""
