#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2010 - 2014 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

################################################################################
# utilities
################################################################################
IQUERY="iquery -c ${IQUERY_HOST:=localhost} -p ${IQUERY_PORT:=1239}"  # required switches

function getDimLen { local aName=$1 ; local dimIdx=$2
    local LEN=`$IQUERY -ocsv -aq "project(filter(dimensions($aName), No=$dimIdx),length)" |
          tail -n1`
    echo $LEN
}

function nRow { local aName=$1
    getDimLen "$aName" 0
}

function nCol { local aName=$1
    getDimLen "$aName" 1
}

function getDimCSize { local aName=$1 ; local dimIdx=$2
    local LEN=`$IQUERY -ocsv -aq "project(filter(dimensions($aName), No=$dimIdx),chunk_interval)" |
          tail -n1`
    echo $LEN
}

function nRowCSize { local aName=$1
    getDimCSize "$aName" 0
}

function getDimStart { local aName=$1 ; local dimIdx=$2
    local LEN=`$IQUERY -ocsv -aq "project(filter(dimensions($aName), No=$dimIdx),start)" |
          tail -n1`
    echo $LEN
}

function nRowStart { local aName=$1
    getDimStart "$aName" 0
}

function nColStart { local aName=$1
    getDimStart "$aName" 0
}

function getCount { local aExpr=$*
    local COUNT=`$IQUERY -ocsv -aq "project(aggregate($aExpr, count(*)),count)" | tail -n1`
    echo $COUNT
}

function getNumInstances {
    local NUM_INSTANCES=`getCount "list('instances')"`
    echo $NUM_INSTANCES
}

# timed iqueries are always -n for now [_n]
# they can be followed by untimed iqueries if data is to be dumped
# (which should never be timed)
TIMEQUERY=0 # 1 to enable timing and printing of the command to stderr
function timequery_n {
    if [ "$TIMEQUERY" != "0" ] ; then
        /usr/bin/time -f '%e s %C' $IQUERY -naq "$*"
    fi
    $IQUERY -naq "$*"
}

DBG=0  # 1 to enable these messages
function dbg {
    if [ "$DBG" != "0" ] ; then
        echo "$0 debug:" $* >&1
    fi
}

function dbgStart {
    if [ "$DBG" != "0" ] ; then
        echo -n "$0 debug:" $* >&1 " ... " # note the -n ... the line does not break
    fi
}

function dbgEnd {
    dbg "Done"
}


################################################################################
# get/check command line arguments
################################################################################
USAGE="$0 EDGE_LENGTHS_MATRIX RESULT_VECTOR_NAME startVertex"
# bellmanFord.sh array startvertex
# the entries of the matrix are the weights of the edges of a directed graph
# The "from" vertex is the row, and the "to" vertex is the column
# If your data is the reverse, simply take the transpose of the matrix


if [ "$#" -lt 3 ]; then
    echo $USAGE >&1
    exit 1
fi
MAT_IN=$1
RESULTNAME=$2
IDX_STARTVERT=$3
TEST_RIGHT_REPLICATE=$4 # for test purposes only, of the form "rightReplicate=true" (or false)

# argument checks:
NROW=`nRow $MAT_IN`
NCOL=`nCol $MAT_IN`
if [ "$NROW" != "`nCol $MAT_IN`" ] ; then
    echo "${0}: MAT_IN $MAT_IN is $NROW by $NCOL, but must be square" >&2
    exit 1
fi


if [ "`nRowStart $MAT_IN`" -ne "0" -o "`nColStart $MAT_IN`" -ne "0" ]; then
    echo "${0}: MAT_IN $MAT_IN must have its dimensions start at 0" >&2
    exit 1
fi

TEST__STR=""
if [ "$TEST_RIGHT_REPLICATE" != "" ] ; then
    TEST_STR=", '${TEST_RIGHT_REPLICATE}'"
fi

################################################################################
# preliminaries
################################################################################

$IQUERY -aq "load_library('linear_algebra')" # for spgemm()

################################################################################
# begin Bellman-Ford (single-source shortest paths) algorithm proper
# bellmanFord(MAT_IN, START_VERTEX)
# e.g. seed GAitLoLA 
################################################################################
# MAT_IN given
# IDX_STARTVERT given
NUM_VERT=`nRow $MAT_IN`
MAX_VERT=$(( ${NUM_VERT}-1 ))      # TODO: assuming 0 is min

# the best chunksize for the matrix-vector multiplication would be:
# (white box knowledge!)
# CSIZE=$((NUM_VERT/`getNumInstances`))
# however, instead of repart-ing, we
# willl just match the input, instead, for now
# [TODO: copy to an optimal chunksize]
CSIZE=`nRowCSize $MAT_IN`

#
# how many iterations are really needed?
# well, enough to propagate through all the connected vertices.
# so we don't need to iterate once for every vertex index, just once
# for every vertex.  So lets count how many vertices are actually
# used in MAT_IN: ... we'll aggregate by v0 to find which v0's are used
NUM_V0_USED=`getCount "aggregate($MAT_IN, sum(w),v0)"`
echo "NUM_V0_USED is $NUM_V0_USED"


#
# similar ... get a vector that shows WHERE v0 has an edge
#             i.e. max(1,sum(w over v1))
#
# weird use of aggregate ... I don't really expect the zeros to show
# up... those entries are implicit zeros in the input.
# what I'm really trying to do is just find the listing of the rows that
# were used.
# TODO: In case weights are negative (not supposed to be for BellmanFord) we could
# throw in an apply(abs(w)) first, or check and reject the input
#
TMP1="BELLMAN_FORD_TMP1_$$"
$IQUERY -aq "remove($TMP1)" >/dev/null 2>&1 # failure expected
timequery_n "store(project(apply(project(aggregate($MAT_IN, sum(w), v0),w_sum),
                                 w, iif(w_sum != 0, float(0),float(-.1))),
                           w),
                   $TMP1)"
USED_V0="BELLMAN_FORD_USED_V0_$$"
$IQUERY -aq "remove($USED_V0)" >/dev/null 2>&1 # failure expected
timequery_n "store(substitute($TMP1,build(<v:float>[dc=0:0,1,0],NaN)),$USED_V0)"


# the following bellman-ford implementation requires all vertices to have
# a distance to themselves of zero.  For sparse multiplication to work,
# the implicit value of the matrix (value where a missing cell is)
# is AS IF the value were the additive identity.  While on normal
# arithmetic, this is is 0, the semiring we are using is
# (add, mult, add-id, mult-id) = (min, +, +inf, 0)
# so the additive identity is +inf.
#
# so here we set the diagonal explicitly to 0
#

#
# there's two ways to do this
# + Matrix * Vector   - faster when nnz(Vector) > nnz(Matrix)
# + Vector' * Matrix' - 
#
# the right-hand matrix is the one that gets dealt with in round-robin style
# so its best to put the larger matrix (nnz count) on the right
# assuming the one on the left will then reduce the number of operations better

# USED_V0_W:
# need a matrix with zero diagonal (to merge with the input matrix, later).
# so we make a list of <v0,v1>[w=0] and redimension it to <w=0>[v0,v1]

ALL_V0_W="BELLMAN_FORD_ALL_V0_W_TMP_$$"   # these are the "pre-diagonals" all V0's and w=0
USED_V0_W="BELLMAN_FORD_USED_V0_W_TMP_$$" # all _used_ V0's + w=0
$IQUERY -aq "remove($ALL_V0_W)" > /dev/null 2>&1
$IQUERY -aq "remove($USED_V0_W)" > /dev/null 2>&1

PAIRS_SCHEMA="<v1:int64>[v0=0:$MAX_VERT,$CSIZE,0]"
timequery_n "store(apply(build($PAIRS_SCHEMA, v0),w,float(0.0)),$ALL_V0_W)"

# USED_V0's dimension is v0, must duplicate this into v1
timequery_n "store(apply($USED_V0,v1,v0),$USED_V0_W)"

# create MAT_DZ: (for diagonal zeros)
# which is the matrix of weights, with a diagonal forced to explicit zero
# (but transposed from $MAT_IN)
MAT_DZ="BELLMAN_FORD_MAT_DZ_TMP_$$"
$IQUERY -aq "remove($MAT_DZ)" > /dev/null 2>&1

# reshape the USED_V0 to the matrix shape, and then merge it
MAT_DIAG_USED_TMP="DIAG_USED_TMP_$$"
MAT_DIAG_USED_TMP2="DIAG_USED_TMP2_$$"
$IQUERY -aq "remove($MAT_DIAG_USED_TMP)" >/dev/null 2>&1 # failure expected
$IQUERY -aq "remove($MAT_DIAG_USED_TMP2)" >/dev/null 2>&1 # failure expected
timequery_n "store(redimension($USED_V0_W,$MAT_IN), $MAT_DIAG_USED_TMP)"
timequery_n "store(merge($MAT_DIAG_USED_TMP,$MAT_IN), $MAT_DIAG_USED_TMP2)"

# now transpose the matrix... note that currently the redimesion is faster
# than this transpose:
#     timequery_n "store(transpose($MAT_DIAG_USED_TMP2), $MAT_DZ)"
MAT_DZ_SCHEMA="<w:float>[v1=0:$MAX_VERT,$CSIZE,0, v0=0:$MAX_VERT,$CSIZE,0]"
$IQUERY -aq  "create array $MAT_DZ $MAT_DZ_SCHEMA"
timequery_n "store(redimension($MAT_DIAG_USED_TMP2,$MAT_DZ), $MAT_DZ)"
# TODO: may be slightly cheaper to do the tranpose prior to the merge, so the
#       diagonal data does not have to pass through the transpose.
# TODO: may be better to have the input be in the transposed form to
#       save time... its probably being put through a redimension by the
#       caller anway, so is probably for free.


# create and initialize SHORTEST (vector of shortest paths)

# intiliaze the shortest paths vector, SHORTEST
# the starting vertex must be set to 0
# and the relaxation propagates from there

# make a single start vertex <v0:$STARTVERT>[dummy]
START_VTX="BELLMAN_FORD_START_TMP_$$"
$IQUERY -aq "remove($START)" > /dev/null 2>&1
START_VTX_SCHEMA="<v0:int64>[dummy=0:0,1,0]"  # a single v1 value, (only 1 dummy index)
# now add a single attribute w, with a value of 0
timequery_n "store(apply(build($START_VTX_SCHEMA, $IDX_STARTVERT), w, float(0)),$START_VTX)"
# so START_VERTEX is <v0=$IDX_STARTVERT, w=0.0>[dummy]

# and redim that into <w:0>[v0=0:$MAX_VERT]
VEC_SHORTEST="BELLMAN_FORD_SHORTEST_TMP_$$"
VEC_SHORTEST_NEXT="BELLMAN_FORD_SHORTEST_NEXT_TMP_$$"

# distances from the last vertex, $MAX_VERT
$IQUERY -aq "remove($VEC_SHORTEST)" > /dev/null 2>&1   # "just run"
$IQUERY -aq "remove($VEC_SHORTEST_NEXT)" > /dev/null 2>&1   # "just run"

VEC_SHORTEST_SCHEMA="<w:float>[v0=0:$MAX_VERT,$CSIZE,0, dummy=0:0,1,0]"  # (column) vector as matrix
$IQUERY -aq  "create array $VEC_SHORTEST $VEC_SHORTEST_SCHEMA"
timequery_n "store(redimension($START_VTX, $VEC_SHORTEST), $VEC_SHORTEST)"
                                                                              

#
# do iterations
#
EPSILON=0.001   # TODO  -- could be passed in
CHECK_ITERS=10  # after every X (>=1) iterations, check for convergence

DIFF_COUNT=99 # aribtrary
for (( II=1; II<=$NUM_V0_USED; II++  )) ; do
    dbg "bellman-ford iteration II=$II ----------------------"
    $IQUERY -naq "store(spgemm($MAT_DZ,$VEC_SHORTEST,'min.+' ${TEST_STR}), $VEC_SHORTEST_NEXT)" # TODO: this should be timed
    STATUS=$?
    if [ "$STATUS" -ne "0" ] ; then
        echo "error in spgemm query" >&2
        exit $STATUS
    fi

    if [[ II -gt 1 && II%CHECK_ITERS -eq 0 ]] ; then
        SHORTEST_NEXT_COUNT=`getCount "$VEC_SHORTEST_NEXT"`  # how many are not converged
        if [[ "$SHORTEST_COUNT" -ne "$SHORTEST_NEXT_COUNT" ]] ; then
            # fast check, can't be equal, so just say 99 are different
            DIFF_COUNT=99
        else
            # same size, could be equal now
            echo "@@@ II is $II, checking how many cells have changed"
            QUERY="filter(apply(join($VEC_SHORTEST as prior,$VEC_SHORTEST_NEXT as next),pct,abs((next.multiply-prior.multiply)/next.multiply)),pct>$EPSILON)"
            DIFF_COUNT=`getCount "$QUERY"`  # how many are not converged
        fi
        if [[ DIFF_COUNT -le 0 ]] ; then
            echo "@@@ stabilized, stopping early at iteration $II" >&2
        #else
        #    echo "ITERATION II=$II, count of entries changed more than EPSILON=$EPSILON is $DIFF_COUNT" >&2
        fi
    fi

    # prepare for next iteration
    $IQUERY -naq "remove($VEC_SHORTEST)"   # just used it
    $IQUERY -naq "rename($VEC_SHORTEST_NEXT, $VEC_SHORTEST)" # replace it
    SHORTEST_COUNT=$SHORTEST_NEXT_COUNT   # reassign count when renaming

    if [[ DIFF_COUNT -le 0 ]] ; then
        break;
    fi
done


$TIME $IQUERY -naq "remove($TMP1)"
$TIME $IQUERY -naq "remove($USED_V0)"
$TIME $IQUERY -naq "remove($ALL_V0_W)"
$TIME $IQUERY -naq "remove($USED_V0_W)"
$TIME $IQUERY -naq "remove($START_VTX)"
$TIME $IQUERY -naq "remove($MAT_DIAG_USED_TMP)"
$TIME $IQUERY -naq "remove($MAT_DIAG_USED_TMP2)"
$TIME $IQUERY -naq "remove($MAT_DZ)"

$TIME $IQUERY -naq "rename($VEC_SHORTEST, $RESULTNAME)"

