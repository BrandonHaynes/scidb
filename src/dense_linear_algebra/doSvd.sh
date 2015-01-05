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
#

#
# TODO:
# add getopts to get switches
# switch to print name of test to stderr during test
# switch to print timing of test to stderr
# switch to print matrices to stderr
# can use a variable to represent "| tee /dev/stderr"  perhaps
#

# Prior versions of this script used variables named SIZE[_xxx] to control the
# size of the problem matrix, which was always square.
# For #2200 we are allowing recrtangular matrices.
# Here, we generally changing SIZE[_XXX] to ORD[_XXX] which
# represents the ORDER of the matrix, and we add variabs ROW[_XXX] and COL_[XXX]
# which will be used to control the aspect ratio of a matrix at a given order
# [Note that the order of a matrix is the maximum of its rows and columns]
# [Note also that we do not change CSIZE, which is the chunkSize, which does remain square at this time]

# defaults if not given on command line
ORD_MIN=2
ORD_MAX_VERBOSE=8
ORD_MAX=8
ORD_STEP_TYPE="x" # or "+"
ORD_STEP=2


if [ -z "$SCRIPT_DIR" ] ; then
    SCRIPT_DIR="src/linear_algebra"
fi

if [ "$1" != "" ] ; then
    ORD_MIN=$1  # TODO: replace this with getopt
    ORD_MAX_VERBOSE=$1
    ORD_MAX=$1
fi

if [ "$2" != "" ] ; then
    ORD_MAX_VERBOSE=$2
    ORD_MAX=$2
fi

if [ "$3" != "" ] ; then
    ORD_MAX=$3
fi

if [ "$4" != "" ] ; then
    ORD_STEP_TYPE=$4
fi

if [ "$5" != "" ] ; then
    ORD_STEP=$5
fi

BASE_SCRIPT_NAME=$(basename $0)

echo "$BASE_SCRIPT_NAME: ====================================="
echo "$BASE_SCRIPT_NAME: $BASE_SCRIPT_NAME $ORD_MIN,$ORD_MAX_VERBOSE,$ORD_MAX begin"

sleep 2 # if we get here too soon after runN.py 4 mydb --istart , then there's a connection error on this command
iquery -aq "load_library('dense_linear_algebra')"

#
# time gesvd operator on matrices of small size, printing their results
#
echo "$BASE_SCRIPT_NAME: **********************************************************************" 
echo "$BASE_SCRIPT_NAME: ****** verbose, remultiplied svd('U'), svd('VT'), svd('values') "
echo "$BASE_SCRIPT_NAME: ****** from $ORD_MIN to $ORD_MAX_VERBOSE (if any)"

PI="3.14159265359"
NAMES_USED="IN LEFT RIGHT VALS VALSmat PRODUCT DIAG_VEC_1 DIAG_OUTER_PRODUCT"
PFX="TEST_DOSVD_"   # array name prefix

ORD=$ORD_MIN
while [ "$ORD" -le "$ORD_MAX_VERBOSE" ] ; do
    ORD_M1=`expr "$ORD" - 1`
    CSIZE=32     # Only CSIZE 32 is accepted at the moment, using variable to allow later generalization (#2200)

    echo "$BASE_SCRIPT_NAME: verbose svd test @ ${ORD} x ${ORD} csize ${CSIZE} x ${CSIZE}"  | tee /dev/stderr

    for NAME in $NAMES_USED ; do
        iquery -aq "remove(${PFX}${NAME})"   > /dev/null 2>&1 # completely silently
    done

    iquery -naq "create array ${PFX}IN <v:double>[r=0:${ORD_M1},${CSIZE},0 , c=0:${ORD_M1},${CSIZE},0]" #> /dev/null

    # test matrix is designed to
    # 1. be integers
    # 2. have no zeros (allows least-significant errors to "disappear")
    # 3. have a condition number better than O^2
    #
    BUILD="build(${PFX}IN, 1+c+r*${ORD})" # numbered by columns
    #iquery -ocsv+ -aq "$BUILD"  | sort       | tee /dev/stderr
    #echo

    #
    # now execute the SVD's  they must be silent because they don't generate
    # the same answer on different #'s of instances, because the matrix used
    # will have eigenvalues near zero. (can I say rank deficient?)
    #
    # remove the n in -naq to see the matrix for debugging
    #
    echo "$BASE_SCRIPT_NAME: verbose U test @ ${ORD} x ${ORD} csize ${CSIZE} x ${CSIZE}"   | tee /dev/stderr
    iquery -ocsv+ -naq "store(gesvd(${BUILD}, 'U'),${PFX}LEFT)"  | sort       #| tee /dev/stderr
    echo                                                                     #| tee /dev/stderr
    echo "$BASE_SCRIPT_NAME: verbose VT test @ ${ORD} x ${ORD} csize ${CSIZE} x ${CSIZE}"  | tee /dev/stderr
    iquery -ocsv+ -naq "store(gesvd(${BUILD}, 'VT'),${PFX}RIGHT)" | sort      #| tee /dev/stderr
    echo                                                                     #| tee /dev/stderr
    echo "$BASE_SCRIPT_NAME: verbose S test @ ${ORD} x ${ORD} csize ${CSIZE} x ${CSIZE}"   | tee /dev/stderr
    iquery -ocsv+ -naq "store(gesvd(${BUILD}, 'S'),${PFX}VALS)"   | sort      #| tee /dev/stderr
    echo                                                               #| tee /dev/stderr

    # convert S vector to a matrix
    ${SCRIPT_DIR}/diag.sh ${PFX}VALS ${PFX}VALSmat ${PFX}DIAG_VEC_1 ${PFX}DIAG_OUTER_PRODUCT
    #echo "$BASE_SCRIPT_NAME: ${PFX}VALSmat:" ;
    #iquery -ocsv+ -aq "scan(${PFX}VALSmat)" | sort                          #| tee /dev/stderr

    iquery -ocsv+ -aq "
     aggregate ( 
      apply(
       cross_join(${PFX}LEFT as C,  
                  aggregate(apply(cross_join(${PFX}VALSmat as A, ${PFX}RIGHT as B, A.c, B.i), s2, A.s * B.v), sum(s2) as multiply, A.i, B.c) as D,
                  C.i, D.i
       ), 
       s, C.u * D.multiply
      ), 
      sum(s) as multiply, C.r, D.c
    )" | sort #| tee /dev/stderr
    echo

    # at small sizes, increment (to catch bugs), at larger sizes, double the size (to scale up faster)
    if [ "${ORD_STEP_TYPE}" = "+" ] ; then
        ORD=`expr "$ORD" '+' "$ORD_STEP"`
    elif [ "${ORD_STEP_TYPE}" = "x" ] ; then
        ORD=`expr "$ORD" '*' "$ORD_STEP"`
    else
        echo "$BASE_SCRIPT_NAME: illegal value for ORD_STEP_TYPE, = ${ORD_STEP_TYPE}"
        exit 5
    fi
done


#
# now run up to a limiting size for performance more than
# edge condition testing
#
echo "$BASE_SCRIPT_NAME: *****************************************************************************" 
echo "$BASE_SCRIPT_NAME: ****** quick test, svd('U') only"
echo "$BASE_SCRIPT_NAME: ****** from $ORD to $ORD_MAX (if any)"

while [ "$ORD" -le "$ORD_MAX" ] ; do
    ORD_M1=`expr "$ORD" - 1`
    # NOPE, the release is too close.  we will just use csize 32 and generalize later.
    CSIZE=32

    echo "$BASE_SCRIPT_NAME: U-only test @ ${ORD} x ${ORD} csize ${CSIZE} x ${CSIZE}" | tee /dev/stderr

    iquery -aq "remove(${PFX}IN)"   > /dev/null 2>&1 # completely silently
    iquery -naq "create array ${PFX}IN <v:double>[r=0:${ORD_M1},${CSIZE},0 , c=0:${ORD_M1},${CSIZE},0]" 
    #
    # see explanation in previous loop
    #
    BUILD="build(${PFX}IN, 1+c+r*${ORD})"

    if [ "$ORD" -ge 16384 ] ; then
        echo "${0}: warning, a 32-bit ScaLAPACK/LAPACK will fail at this size"
        echo "${0}: with error 'xxmr2d: out of memory'"
        # NOTE LWORK is 532 MB still 4x short of the 2G barrier, fails earlier than i expect
    fi

    # only elapsed/real time E makes sense from iquery
    echo "aggregate(gesvd(${BUILD}, 'U'),count(*))"    | tee /dev/stderr
    /usr/bin/time -f'%E s' iquery -ocsv+ -aq "aggregate(gesvd(${BUILD}, 'U'),count(*))"    | tee /dev/stderr
    echo                                                      #| tee /dev/stderr

    if [ "${ORD_STEP_TYPE}" = "+" ] ; then
        ORD=`expr "$ORD" '+' "$ORD_STEP"`
    elif [ "${ORD_STEP_TYPE}" = "x" ] ; then
        ORD=`expr "$ORD" '*' "$ORD_STEP"`
    else
        echo "$BASE_SCRIPT_NAME: illegal value for ORD_STEP_TYPE, = ${ORD_STEP_TYPE}"
        exit 5
    fi
done

for NAME in $NAMES_USED ; do
    iquery -aq "remove(${PFX}${NAME})"   > /dev/null 2>&1 # completely silently
done

echo "$BASE_SCRIPT_NAME: $BASE_SCRIPT_NAME $ORD_MIN,$ORD_MAX_VERBOSE,$ORD_MAX end"
echo 

