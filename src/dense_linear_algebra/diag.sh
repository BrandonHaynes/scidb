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
#echo "$0: begin ------------------------------------"
#set -x

#######################################
# was source src/linear_algebra/iqfuncs.bash
#######################################
IQFUNCS_TRACE=""
TMPTIME=/tmp/time.$$
TIME="time -f %E -o $TMPTIME"
showtime() {
    echo '    real ' `cat $TMPTIME`
}

afl() {
    if [ "$IQFUNCS_TRACE" ] ; then
        #set -x
        echo -n "afl $* -> "
        $TIME iquery -a $*
        showtime
        #set +x
    else
        iquery -a $*
    fi
}

nafl() {
    if [ "$IQFUNCS_TRACE" ] ; then
        #set -x
        echo "nafl $* "
        $TIME iquery -na $* | grep -v 'Query was executed successfully'
        showtime
        #set +x
    else
        iquery -na $* | grep -v 'Query was executed successfully'
    fi
}

eliminate() {
    if [ "$IQFUNCS_TRACE" ] ; then
        #set -x
        echo "eliminate $1"  # no -n
        iquery -naq "remove(${1})" 2> /dev/null | grep -v 'Query was executed successfully'
        #set +x
    else
        iquery -naq "remove(${1})" 2> /dev/null | grep -v 'Query was executed successfully'
    fi
}

# convert adddim(array,dimension) to redimension(apply())
getAdddim() {
    MY_SCHEMA=`iquery -c $IQUERY_HOST -p $IQUERY_PORT -o dense -aq "show($1)"`
    MY_SCHEMA=${MY_SCHEMA:2:(${#MY_SCHEMA}-4)}

    ATTR_S=`expr index "${MY_SCHEMA}" \<`
    DIM_S=`expr index "${MY_SCHEMA}" \[`
    DIM_E=`expr index "${MY_SCHEMA}" \]`
    RES="redimension(apply($1,$2,0),<${MY_SCHEMA:$ATTR_S:$DIM_S-$ATTR_S}$2=0:0,1,0,${MY_SCHEMA:$DIM_S:$DIM_E-$DIM_S})"
    echo $RES
}

######################
# end was iqfuncs.bash
######################

if [ "$#" -ne 4 ] ; then
    echo "$0: numargs is $#"                                 >&2
    echo "usage: $0 VEC_IN RESULT TMP_VEC_1 TMP_OUTER_PRODUCT"   >&2
    echo "# converts a vector to a (n,1) matrix"             >&2
    exit 2
fi

VEC_IN=$1
RESULT=$2
TMP_VEC_1=$3
TMP_OUTER_PRODUCT=$4

# get length of VEC_IN
LENGTHS=`iquery -o dense -aq "project(dimensions(${VEC_IN}),length)"`

# NOTE: there was a suggestion that maybe -ocsv[+] might make the parsing below easier
#COLS="${LENGTHS:2:-2}"
# bash 4.2.x accepts the -2, above.  
# bash 4.1.5 does not like the -2, so explicitly subtract instead
SUBSTR_LEN=`expr length ${LENGTHS} - 4` # length that will drop trailing ")]"
COLS="${LENGTHS:2:$SUBSTR_LEN}"         # 2: is to drop the leading "[("

CHUNK_INTERVAL=`iquery -o dense -aq "project(dimensions(${VEC_IN}),chunk_interval)"`
SUBSTR_LEN=`expr length ${CHUNK_INTERVAL} - 4` # length that will drop trailing ")]"
CHUNKSZ="${CHUNK_INTERVAL:2:$SUBSTR_LEN}"

# generate a vector equal to the values and a row vector of 1
# and multiply them together.  They will RLE nicely.
#   [ a ]                      [ a a a ]
#   [ b ]       x  [ 1 1 1 ] = [ b b b ]
#   [ c ]                      [ c c c ]
#
#   t(addim(VEC)) * TMP_VEC_1 -> TMP_OUTER_PRODUCT
#

eliminate "${TMP_VEC_1}"

COL_MAX=$((${COLS}-1))

#
# make a ROW VECTOR of 1's
#
 #echo "iquery -naq $* "  # NOTE: creates cannot be done with nafl until quoting is fixed
iquery -naq "create array ${TMP_VEC_1} <v:double>[r=0:0,1,0,c=0:${COL_MAX},${CHUNKSZ},0]" \
    | grep -v 'Query was executed successfully'
nafl -q "store(build(${TMP_VEC_1},1),${TMP_VEC_1})"

eliminate "${TMP_OUTER_PRODUCT}"
ADDDIM_Q=$(getAdddim "${VEC_IN}" "c")

if false ; then
    echo "debug diag transpose(adddim(VEC_IN)) @@@@@@@@@@@@@@@@"
    set -x
    afl -q "transpose(${ADDDIM_Q})"
    eliminate "DEBUG_1"
    afl -q "store(transpose(${ADDDIM_Q}),DEBUG_1)"
    afl -q "show(DEBUG_1);"
    echo "debug diag transpose ${TMP_VEC_1} @@@@@@@@@@@@@@@@"
    afl -q "show(${TMP_VEC_1})"
    set +x
    echo "debug diag end @@@@@@@@@@@@@@@@"
fi

#
# make a vector from the original vector with the "add dim" operator, and multiply it
# ... or use gemm, or just use redimension, buddy!

iquery -naq "store(aggregate(apply(cross_join(transpose(${ADDDIM_Q}) as A, ${TMP_VEC_1} as B, A.c, B.r), s, A.sigma*B.v), sum(s) as multiply, A.i, B.c),${TMP_OUTER_PRODUCT})" >> /dev/null

# then we just use iif to set off-diagonal values to 0
#  
#
#    [ a 0 0 ]
# -> [ 0 b 0 ] which is our result
#    [ 0 0 c ]

eliminate "${RESULT}"
nafl -q "store(project(apply(${TMP_OUTER_PRODUCT},s,iif(i=c,multiply,0.0)),s),${RESULT})"

#echo "$0: end ------------------------------------"
exit 0

