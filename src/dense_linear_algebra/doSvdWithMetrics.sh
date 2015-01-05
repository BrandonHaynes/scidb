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
# doSvd.sh
#

#######################################
# was source src/linear_algebra/iqfuncs.bash
# TODO: use SCRIPT_DIR and go back to sourcing it
# NOTE: this version has an enhacement at NOTE
#######################################
IQFUNCS_TRACE=""
TMPTIME=/tmp/time.$$
TIME="time -f %E -o $TMPTIME"
showtime() {
    if [ ! -z "$TIME" ] ; then    # NOTE: this is an improvement over original
        echo '    real ' `cat $TMPTIME`
    else
        true ; # echo 'timing off' # NOTE: chage from original 
    fi
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

#set -x 

BASE_SCRIPT_NAME=$(basename $0)

echo "$BASE_SCRIPT_NAME: begin ------------------------------------"

# settings for iqfuncs above
IQFUNCS_TRACE="y"  # debugging info
TIME=""            # don't time the commands would make checkin tests fail 

if [ -z "$SCRIPT_DIR" ] ; then
    SCRIPT_DIR="src/linear_algebra"
fi


countOrPrint() {
    local QUERY="$*"
    #set -x
    if [ "$ORDER" -gt "$ORDER_DETAIL_LIMIT" ] ; then
        afl -q "aggregate(${QUERY},count(*))"
    else
        afl "${IQFMT}" -q "${QUERY}"
    fi
    #set +x
}

nafl -q "load_library('dense_linear_algebra')"
echo

#set formatting of iquery output by iqfuncs
IQFMT="-ocsv+"   # explicit row, column, value ... but not in any particular order, which is ok
#IQFMT="-osparse" # good for debug ... shows the distribution on the nodes, not for checkin tests


#
# should have a getopt to start things off.  right now, just used fixed arguments
#
TEST=$1   # main loop serves many different inner tests


# size of matrix and chunk size
# at some point we'll want to pass this in to generate mutliple tests

# parameters of the following routines
ORDER_DETAIL_LIMIT=4
# CSIZE    matrix chunksize
# ORDER    order of the test matrix

###############################################################
# TODO: make a wrapping script that will call this script after
#       shutdown and restart SciDB after changing test/runtests
#       tee the output of each test into a file doSvd.pNNN.txt
#       then we can look for anomalize and scale up to large sizes
###############################################################

#
# NOTE: a good test is ORDER in 2 3 ; M_CNK in 1 2 3 4
#  this makes order less than and greater than CSIZE which is good for testing
#  all the edge conditions of partial chunks, where there was a lot of trouble
# it would be great if these ranges could be specified to doSvd.sh
# may want to start writing the doXXX.sh in python, as shell is getting painful
# may be able to use python connector to run the tests
#


# bug: ORDER 1 does not work even on old SVD

# useful ORDER_LISTs:
#
# sizes, thoroughness
# -------,----------
XLARGE_SIZE_LIST="16384 32768 65536"   # requires an ILP64 BLAS
LARGE_SIZE_LIST="                                         128 256 512 1024 2048 4096 8192"
MEDIUM_SIZE_LIST="2 3 4 5 6 7 8 9 10 12 14 16 17 20 32 64 128 256 512"
SMALL_SIZE_LIST=" 2 3 4       8 9 10                32    128     512"
SMALLER_SIZE_LIST="3       7                  17    32    128     512"
SMALLER2_SIZE_LIST="3                         17       64         512"
SMALLEST_SIZE_LIST=" 4     7                           64"
#
#ORDER_LIST=${SMALL_SIZE_LIST}
ORDER_LIST=${SMALLEST_SIZE_LIST} # for checkin test

# TODO: Tigor .. Python version should probably get the list from a command line argument

echo "@@@@ $BASE_SCRIPT_NAME: @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "@@@@ $BASE_SCRIPT_NAME: matrix orders: $ORDER_LIST"

for ORDER in $ORDER_LIST
do

  PROD_CHUNK_SIZE=64  # likely candidates for production are 32,64,128
  CHUNKSIZES=$PROD_CHUNK_SIZE # start off with that one

  if true; then    # run "full" test, testing lots of edge conditions
                   # by setting the chunksize to make edge conditions
                   # and to ensure a minimum amount of multi-instance
                   # distribution is involved, by dividing into
                   # roughly 2,3,4, .. etc pieces

      # white box test: also test chunksizes near the matrix size
      #
      # _ separates the numbers,... easier to tr to newlines for sorting with sort -nu
      # for now, we are not going to go above chunksize 64 ... that may be
      # the limit I set in the operator
      if [ "$ORDER" -lt $(($PROD_CHUNK_SIZE)) ] ; then
          # add on ORDER-1, ORDER, and ORDER+1 sizes (boundary conditions)
          CHUNKSIZES="${CHUNKSIZES}_$((${ORDER}-1))_${ORDER}_$((${ORDER}+1))"
      elif [ "$ORDER" -le $(($PROD_CHUNK_SIZE)) ] ; then
          # test is same as "equal"
          # add on ORDER-1 and ORDER ... ORDER+1 is over the limit
          CHUNKSIZES="${CHUNKSIZES}_$((${ORDER}-1))_${ORDER}))"
      else
          # just ORDER itself
          CHUNKSIZES="${CHUNKSIZES}_${ORDER}))"
      fi

      # black box test: divide the size into N chunks ..
      # this is away from the edge conditions, but without exhaustive testing
      # TODO: Tigor this list, e.g 7,3 or 7,4,3 should be an argument to the Python version
      # TODO: Make number of divs controllable from outside, right now config for a checkin test
      #       was DIVS in 7 3
      for DIVS in 2 ; do           # must get above the number of instances to see full parallelism
          TMP=$((${ORDER}/${DIVS}))
          if [ "$TMP" -ge 2 ]; then
              if [ "$TMP" -le 64 ]; then
                  CHUNKSIZES="${CHUNKSIZES}_${TMP}"
              fi
          fi
      done

      # remove duplicate chunk sizes only to save testing time
      echo "CHUNKSIZES pre-sort: $CHUNKSIZES"
      CHUNKSIZES=`echo $CHUNKSIZES | tr _ '\n' | sort -nu | tr '\n' ' ' `
  fi

  for CSIZE in $CHUNKSIZES ; do

    # make a (ORDER, ORDER) matrix
    NROW=$ORDER
    NCOL=$ORDER
    MAXROW=$((${NROW}-1))
    MAXCOL=$((${NCOL}-1))

    # want to use "nafl" from iqfuncs.bash, but haven't resolved quoting problems yet
    eliminate INPUT
    iquery -aq "create array INPUT <v:double>[r=0:${MAXROW},${CSIZE},0 , c=0:${MAXCOL},${CSIZE},0]"  | grep -v 'Query was executed successfully'

    # TODO: Tigor, no need for this in Python version
    if [ ${TEST} == "mpicopy" -o ${TEST} == "mpirank" ] ; then
        # hard-coded artificial limit on size of these tests, until limit is an argument
        if [ "$ORDER" -le "128" ] ; then # really for debugging, run enough just to keep this case working
            # tests  using test functions:  mpicopy, mpirank
            # drive them with cells containing their column-major numbering (good for debugging)
            echo -n "create test data for copy/rank" ; countOrPrint "store(build(INPUT,r*{NCOL}+c,INPUT)"

            if [ "$TEST" == "mpicopy" ] ; then
                echo "@@@@ $BASE_SCRIPT_NAME: -----------------------------------------------"     #|tee /dev/stderr
                echo "@@@@ $BASE_SCRIPT_NAME: mpi(scalapack)copy , size ${ORDER}, csize ${CSIZE}"  #|tee /dev/stderr
                # test that arrays written to ScaLAPACK memory and copied in the
                # slave are then transferred from memory back to arrays correctly
                eliminate IN_COPY
                echo -n "mpicopy: " ; countOrPrint "mpicopy(INPUT)"
                echo -n "store mpicopy: " ; countOrPrint "store(mpicopy(INPUT),IN_COPY)"
            fi

            if [ "$TEST" == "mpirank" ] ; then
                # test that arrays written to ScaLAPACK memory goto and come from
                # the mpi rank / ScaLAPACK grid coordinate that we expected
                echo "@@@@ $BASE_SCRIPT_NAME: -----------------------------------------------"     #|tee /dev/stderr
                echo "@@@@ $BASE_SCRIPT_NAME: mpi(scalapack)rank , size ${ORDER}, csize ${CSIZE}"  #|tee /dev/stderr

                eliminate CHECK
                echo -n "diff from original:";
                countOrPrint "store(apply(join(INPUT,IN_COPY),mag_err,abs(copy-v)),CHECK)"

                echo "$BASE_SCRIPT_NAME: error metric: @=================================@" ;
                echo -n "@@@@ error count:"; afl -q"sum(CHECK,mag_err)"
                # TODO: the above should make a nasty error if input doesn't match the destination rank
                #       or if any input is missing

                echo -n "mpirank: " ; countOrPrint "mpirank(INPUT)"
                echo -n "mpirank: " ; countOrPrint "mpirank(mpirank(INPUT))"
            fi
        fi
    fi

    echo "@@@@ $BASE_SCRIPT_NAME: ---------------------------------------"                       #|tee /dev/stderr
    #
    # test matrix is designed to
    # 1. be integers
    # 2. have no zeros (allows least-significant errors to "disappear")
    # 3. have a condition number better than O^2
    #
    BUILD="build(INPUT,1+c+r*${NCOL})" # row-major numbering

    # TODO: Tigor .. python version only needs to run gesvd .. but follow the else block
    # TODO: James ... if you don't store the output of gesvd .. it seems to break things
    if [ ${TEST} == "svd" ] ; then
      if [ "$ORDER" -gt "2048" ] ; then
        echo "@@@@ $BASE_SCRIPT_NAME: SVD(A) size ${ORDER}, chunksize ${CSIZE}"   |tee /dev/stderr
        # don't bother saving and running SvdMetric,
        # plain svd is too slow to compare to anyway

        echo "$BASE_SCRIPT_NAME: building test matrix"
        nafl -q "store(${BUILD},INPUT)"

        echo "$BASE_SCRIPT_NAME: compute 3 svd matrices: values, left, right, no metrics (too large):" 
        countOrPrint "gesvd(INPUT,'values')"   # will count at this size
        echo -n "." 1>&2
        countOrPrint "gesvd(INPUT,'left')"
        echo -n "." 1>&2
        countOrPrint "gesvd(INPUT,'right')"
        echo -n "." 1>&2
        echo        1>&2
      else
        echo "$BASE_SCRIPT_NAME: svd with check, size ${ORDER}, csize ${CSIZE}"    |tee /dev/stderr
        echo "$BASE_SCRIPT_NAME: building test matrix"
        nafl -q "store(${BUILD},INPUT)"

        eliminate S
        eliminate U
        eliminate VT
        nafl -q "store(gesvd(INPUT,'values'),S)"
        echo -n "." 1>&2
        nafl -q "store(gesvd(INPUT,'left'),U)"
        echo -n "." 1>&2
        nafl -q "store(gesvd(INPUT,'right'),VT)"
        echo -n "." 1>&2
        echo        1>&2

        echo "$BASE_SCRIPT_NAME: calculate metrics "
        # matrix of residual measures: U*S*VT(from svd) - INPUT (the original matrix)
        ${SCRIPT_DIR}/doSvdMetric.sh INPUT S U VT DOSVD_METRIC

        # TODO: problem ... we have vector norm, does not accept matrices
        SUM_SQUARES='aggregate(project(apply(DOSVD_METRIC,square,nelsb*nelsb),square),sum(square))'
        #echo DEBUG SUM_SQUARES: iquery -aq "${SUM_SQUARES}"                           1>&2
        #echo DEBUG rms_err                                                            1>&2
        #iquery -ocsv+ -aq          "apply(${SUM_SQUARES}, rms_err_ulp, sqrt(square_sum))" 1>&2

        # should really divide the norm(ERROR-MATRIX) by the rms(INPUT)
        # instead, I already divided by the size of each entry, so I treat this portion as
        # if the matrix were all 1's, so I divide by ORDER^2 which is the number of those
        # 1's
        ORDER_SQ=$((${ORDER}*${ORDER}))

        #echo DEBUG  metric                                                             1>&2
        #iquery -ocsv+ -aq          "apply(${SUM_SQUARES}, metric, sqrt(square_sum)/${ORDER_SQ})" 1>&2

        eliminate METRIC

        # NOTE: metric cannot be printed to the harness .out file (stdout here),
        #       as it varies with (1) the number of instances
        #       and (2) non-determinism of reduction operations in mpi due to floating point
        #       accumulation in varying orders.
        nafl -q "store(project(apply(${SUM_SQUARES},metric,sqrt(square_sum)/${ORDER_SQ}),metric),METRIC)"
        #echo DEBUG  "scan(METRIC)"                                                     1>&2
        #iquery -aq  "scan(METRIC)"                                                     1>&2

        ERROR_LIMIT=256   # 8 bits
        iquery -aq "scan(METRIC)" >&2
        echo "PASS? (error < ${ERROR_LIMIT})? for size ${ORDER}, csize ${CSIZE}:"
        afl -q "project(apply(METRIC,PASS,metric<${ERROR_LIMIT}),PASS)"                 |tee /dev/stderr
        echo "======================================================================"
      fi # if test $ORDER -gt ...
    fi # if test is SVD
  done
done

eliminate METRIC
eliminate INPUT
eliminate S
eliminate U
eliminate VT
eliminate DOSVD_METRIC


echo "$BASE_SCRIPT_NAME: end ------------------------------------"
