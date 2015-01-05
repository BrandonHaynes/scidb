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

#######################################
# was: source iqfuncs.bash
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
######################
# end was iqfuncs.bash
######################

BASE_SCRIPT_NAME=$(basename $0)

echo "doSvdMetric begin -----------------------------------------"


SIZE_LIMIT=16 # default if not given on command line
if [ -z "$SCRIPT_DIR" ] ; then
    SCRIPT_DIR="src/linear_algebra"
fi

if [ "$#" -ne 5 ] ; then
    #echo "$BASE_SCRIPT_NAME: numargs is $#"
    echo "usage: $BASE_SCRIPT_NAME ORIGINAL S U VT OUTPUT_METRICS" >&2  # all array names
    exit 2
fi


MAT_IN=$1
VEC_S=$2
MAT_U=$3
MAT_VT=$4
METRICS=$5

if false;
then
    echo "MAT_IN is $MAT_IN"
    echo "VEC_S is $VEC_S"
    echo "MAT_U is $MAT_U"
    echo "MAT_VT is $MAT_VT"
    echo "METRICS is $METRICS"
fi

IQFTM=""
#IQFMT="-ocsv+"   # for explicit row,column printing
#IQFMT="-osparse" # enable this when debugging distribution issues

# multiply the factors back together
# first must turn the S vector into a diagonal matrix
TRACE="" ${SCRIPT_DIR}/diag.sh ${VEC_S} METRIC_TMP_DIAG_SS TMP_VEC_1 TMP_OUTER_PRODUCT

#echo -n METRIC_TMP_DIAG_SS"; iquery ${IQFMT} -aq "show(METRIC_TMP_DIAG_SS)"
#echo -n "${MAT_U}";    iquery ${IQFMT} -aq "show(${MAT_U})"
#echo -n "${MAT_VT}";   iquery ${IQFMT} -aq "show(${MAT_VT})"
eliminate METRIC_TMP_PRODUCT
nafl -q "store(multiply(${MAT_U},multiply(METRIC_TMP_DIAG_SS,${MAT_VT})),METRIC_TMP_PRODUCT)"

# difference of |original| and the |product of the svd matrices|
eliminate ${METRICS}
# 2^55= 36028797018963968 ~= reciprocal of 1 LSB
nafl -q "store(apply(join(${MAT_IN},METRIC_TMP_PRODUCT),nelsb,(multiply-v)/abs(v)*36028797018963968.0),${METRICS})"


#TODO: review the number of fields being printed, there are a lot of columns.
#      Use project to keep only the ones we want

eliminate METRIC_TMP_DIAG_SS
eliminate METRIC_TMP_PRODUCT
eliminate TMP_VEC_1
eliminate TMP_OUTER_PRODUCT

#output in ${METRICS}
echo "$BASE_SCRIPT_NAME: output is in array ${METRICS}"

#set +x
echo "$BASE_SCRIPT_NAME: doSvdMetric end -----------------------------------------"
exit 0

