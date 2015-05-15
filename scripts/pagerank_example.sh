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

function getValue { local aqlExpr=$1 
    local VAL=`$IQUERY -ocsv -aq "$aqlExpr" | tail -n1`
    echo $VAL
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
USAGE="$0 GRAPH_ADJACENCY_MATRIX PAGERANK_VECTOR_ITERATION"
# e.g. pagerank_example.sh GRAPH_ADJACENCY_MATRIX PAGERANK_I
# the entries of the matrix are the probabilities of a link leaving
# a "from" vertex and arriving at a "to" vertex
# The "from" vertex is the columns, and the "to" vertex is the rows
# If your data is the reverse, simply redimension before calling this script
# NOTE: to check, your columns should sum to 1.0
# although you may omit (sparse matrix) values in columns which have
# no outgoing links, which makes them equivalent to zero when
# processed by this script


if [ "$#" -lt 2 ]; then
    echo $USAGE >&2
    exit 1
fi
MAT_IN=$1
RESULTNAME=$2
TEST_RIGHT_REPLICATE=$3 # for test purposes only, of the form "rightReplicate=true" (or false)

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

TEST_STR=""
if [ "$TEST_RIGHT_REPLICATE" != "" ] ; then
    TEST_STR=", '$TEST_RIGHT_REPLICATE'"
fi

################################################################################
# preliminaries
################################################################################

$IQUERY -aq "load_library('linear_algebra')" # for spgemm()

################################################################################
# begin pagerank
################################################################################
#
# we'll need the transposed matrix (the "from vertex" is name of the columns) for the iterations
#
# MAT_IN given

#
# pagrank computation notes
# let H =$MAT_IN ... i.e. columns of H sum to 1 except where a page has
# no outgoing links (so the column is all 0)
# let S = H with the zero columns replaced by columns of u=1/n, ***
#  i.e. uniform probability of going to any page
# and to keep the "random surfer" from being trapped in a strongly connected component,
# then we add a damping factor alpha, and a constant matrix E with uniform probability of going anywhere
# i.e. full of the columns we added to H
# using these defintions, we can define the "google matrix", G:
# G = alpha S + (1-alpha)E
# G is suitable for applying the Perron-Frobenius theorem, by which we know a
# unique right eigenvector (called the perron vector, P) exists
# (that is, the solution of G*PI = PI corresponding to the largest eigenvalue)
# 
# Since the Perron-Frobenius theorem applies, we can use the power method to converge on this eigenvector
# compute Q_k+1 = G*Q_k ; Q_0 = t(1/n,1/n,...), or the uniform probability distribution across all vertices
#
# computing Q_k:
# H is sparse, and G is not, so we want to do the computation in terms of H in a way
# limiting any dense portion of the calculation to vectors, (or repeated use of a scalar where possible)
#     Q_k+1 = G Q_k
#           = [alpha S + (1-alpha)E ] Q_k
#           = [alpha (H+T) + (1-alpha)E ] Q_k   ; where T= uniform columns as in *** above
# however, note that the second term is a scalar, and the third term is a vector with identical entries
# therefore, these three terms can be reduced to a sparse vector Q_k+1 + one vector of C_k+1 = c*(1,1,1,1,...) for some scalar c
# Letting  Q_i = Q_i + C_i  and substituting for Q_k and Q_k+1 we get:
#     Q_k+1 + C_k+1 = alpha H (Q_k+C_k))+ alpha T (Q_k+C_k) + (1-alpha) E (Q_k+C_k)
# Rearranging to group the vectors and " vectors":
#     Q_k+1         = alpha H Q_k +       alpha (T (Q_k+C_k))where Q_0 + (1-alpha) E Q_k
#             C_k+1 = alpha H C_k +       alpha (T (Q_k+C_k))where !Q_0 + (1-alpha) E C_k
# Note that the c_k term cannot affect the relative order of the result, so does not need to be computed
# Therefore we don't need to compute the c_i portion at all, as it does not affect the relative rankings
# so we will just compute Q_i, and sort the vertices by that
# Note we could compute c_i with time proportional to the number of "zero columns" in MAT_IN' in each pass;
# however, matrices such as the twitter data have a very high number of zero columns, so it is wasteful
# to compute c_i
#

# for more details, see, for example, one of the following:
# + Franceschet, M., "PageRank: Standing on the Shoulders of Giants", arXiv:1002.2858v3 [cs.IR] 14 Aug 2010
# + Spizzirri, L., "Justification and Application of Eigenvector Centrality", 6 March 2011
#

#
# for now, we'll assume that's the case, and later we can
# make the check or renormalize here

EPSILON=0.03   # TODO -- could be passed in, but clamp to the order of the matrix
ITER_LIMIT=60  # TODO -- could be passed in, but most graphs will converge by 43 or so according to one analysis
ALPHA=0.85     # TODO -- could be passed in. 0.85 is the "standard"


# the best chunksize for the matrix-vector multiplication might be:
# CSIZE=$((NUM_VERT_USED/`getNumInstances`))
# but no wider than a chunk size where the sparse accumulator won't fit in cache
# for now, just match the input chunksize, and let it determine it
# [TODO: copy to an optimal chunksize]
CSIZE=`nRowCSize $MAT_IN`

# misc derived values
BETA=`echo "scale=16;1.0-$ALPHA" | bc`              # 1-ALPHA
ALPHA__NROW=`echo "scale=16; $ALPHA/$NROW" | bc`   # a few v0's could lead to all v1s, so max size is more stable?
BETA__NROW=`echo "scale=16; $BETA/$NROW" | bc`   # a few v0's could lead to all v1s, so max size is more stable?
INV_NROW=`echo "scale=16; 1.0/$NROW" | bc`       # a few v0's could lead to all v1s, so max size is more stable?

# Answer the question:
# which vertices have in-links and which have out-links
# and which are used at all?
# we'll aggregate along v1 to find which v0's link TO them
# and we'll aggregate along v0 to find which v1's are linked TO
# The union of both is the "ACTIVE" set of vertices which must be handled
# by matrix multiplication.
# The "inactive" vertices must still be handled, but only require vector operations
# Since there can be a lot of them (e.g. in the twitter case), it is worth splitting
# them out, rather than by using sparse-matrix * dense-vector multiplication
# This lets us do sparse-matrix * sparse-vector and speed the vector data loading
# considerably.

# aggregating will introduce undesired NULLABLE attributes which can't occur in practice
# and these can be eliminated by doing substitute(ARRAY, $NULL_ELIMINATOR_Q) below
NULL_ELIMINATOR_Q="build(<dontcare1:double>[dontcare2=0:0,1,0],NaN)"

LAST_VERT=$(( ${NROW}-1 ))      # TODO: assuming 0 is min

# common schema for the Q_i vector and its sub-terms while computing it
Q_VEC_SCHEMA="<q:float>[vtx=0:$LAST_VERT,$CSIZE,0, vectorDummy=0:0,1,0]"

#
# generate the vector of non-zero columns, NZ_COLS
# and the vector of non-zero columns OR rows, ACTIVE_H (the only ones that multiplying by H can affect)
#

# if we can assume matrix is sparse (no explicit zeros) then the iif() is not necessary
# TODO: pre- filter() MAT_IN to remove zeros, in case it was produced in a dense manner
#                     to speed things up
# TODO: get rid of 0's in ANY matrix/vector that might have dense zeros to speed behavior

# NOTE: agggregate is pretty fast, e.g. 2.5s on twitter data
# these are sparse matrices (assuming $MAT_IN is sparse)
COL_SUM_Q="substitute(aggregate($MAT_IN, sum(prob),v0), $NULL_ELIMINATOR_Q)"
ROW_SUM_Q="substitute(aggregate($MAT_IN, sum(prob),v1), $NULL_ELIMINATOR_Q)"

NZ_COLS_Q="project(apply($COL_SUM_Q, nonzero, iif(prob_sum <> 0.0,int8(1),int8(0))), nonzero)"
NZ_ROWS_Q="project(apply($ROW_SUM_Q, nonzero, iif(prob_sum <> 0.0,int8(1),int8(0))), nonzero)"
ACTIVE_H_Q="project(apply(merge($NZ_COLS_Q,$NZ_ROWS_Q),active,nonzero),active)"

NZ_COLS="PAGERANK_NZ_COLS_$$"
$IQUERY -naq "remove($NZ_COLS)" 2>/dev/null         # failures expected
timequery_n "store($NZ_COLS_Q,$NZ_COLS)"

ACTIVE_H="PAGERANK_ACTIVE_H_$$"
$IQUERY -naq "remove($NZ_ACTIVE)" 2>/dev/null         # failures expected
timequery_n "store($ACTIVE_H_Q,$ACTIVE_H)"

#
# now a vector of where there ARE zero columns AND ACTIVE_H is true
# these are the "dead end" vertices which must have their weight redistirbuted uniformly elsewhere
# otherwise, they can keep reducing the entries of the Perron vector, so you can't get convergence
# by the power method if enough of such vertices would be high-ranking
#

# NOTE: ZERO_COL_0_Q is dense
ZERO_COLS_0_Q="filter(merge($NZ_COLS, build($NZ_COLS,0)), nonzero=0)"
# but ZERO_COL_1_Q is back to sparse, via filter 
ZERO_COLS_Q="filter(project(apply(${ZERO_COLS_0_Q},zero, iif(nonzero=0,int8(1),int8(0)) ),zero), zero <> 0)"
ZERO_COLS_ACTIVE_Q="filter(project(apply(join($ZERO_COLS_Q as ZC,$ACTIVE_H as AH),zcActive,ZC.zero*AH.active),zcActive), zcActive <> 0)"

ZERO_COLS_ACTIVE="PAGERANK_ZERO_COLS_ACTIVE_$$"
$IQUERY -naq "remove($ZERO_COLS_ACTIVE)" 2>/dev/null         # failures expected
timequery_n "store($ZERO_COLS_ACTIVE_Q,$ZERO_COLS_ACTIVE)"

#
# and now the matrix version of the three vectors above 
#
NZ_COLS_VEC="PAGERANK_NZ_COLS_VEC_$$"
$IQUERY -naq "remove($NZ_COLS_VEC)" 2>/dev/null         # failures expected
timequery_n "store(redimension(project(apply($NZ_COLS,
                                             q, float(nonzero),
                                             vtx, v0,
                                             vectorDummy, 0),
                                       q, vtx, vectorDummy),
                               $Q_VEC_SCHEMA),                                       
                   $NZ_COLS_VEC)"
ACTIVE_H_VEC="PAGERANK_ACTIVE_H_VEC_$$"
$IQUERY -naq "remove($ACTIVE_H_VEC)" 2>/dev/null         # failures expected
timequery_n "store(redimension(project(apply($ACTIVE_H,
                                             q, float(active),
                                             vtx, v0,
                                             vectorDummy, 0),
                                       q, vtx, vectorDummy),
                               $Q_VEC_SCHEMA),                                       
                   $ACTIVE_H_VEC)"
ZERO_COLS_ACTIVE_VEC="PAGERANK_ZERO_COLS_ACTIVE_VEC_$$"
$IQUERY -naq "remove($ZERO_COLS_ACTIVE_VEC)" 2>/dev/null         # failures expected
timequery_n "store(redimension(project(apply($ZERO_COLS_ACTIVE,
                                             q, float(zcActive),
                                             vtx, v0,
                                             vectorDummy, 0),
                                       q, vtx, vectorDummy),
                               $Q_VEC_SCHEMA),                                       
                   $ZERO_COLS_ACTIVE_VEC)"

# NUM_ACTIVE is less that NUM_ROW, so we won't loose value in the active
#            vertices by normalizing the re-distributions (for trapped and teleport weight)
#            to a larger number of pages than will receive the weight
NUM_ACTIVE=`getCount "$ACTIVE_H"`
ALPHA__NACTIVE=`echo "scale=16; $ALPHA/$NUM_ACTIVE" | bc`   # a few v0's could lead to all v1s, so max size is more stable?
BETA__NACTIVE=`echo "scale=16; $BETA/$NUM_ACTIVE" | bc`   # a few v0's could lead to all v1s, so max size is more stable?


#
# TODO: assert relationships between NUM_NZ and NUMZ_ COLUMNS (sum == total?)
#

#
# init Q_0 with 1/NROW
#
Q_VEC="PAGERANK_Q_VEC_$$"                      # the current one, Q_k
Q_VEC_NEXT="PAGERANK_Q_VEC_NEXT_$$"            # the next one, Q_k+1
$IQUERY -naq "remove($Q_VEC)" 2>/dev/null              # failures expected
$IQUERY -naq "remove($Q_VEC_NEXT)" 2>/dev/null         # failures expected
timequery_n "store(redimension(project(apply($ACTIVE_H,
                                             q, float(active)*float($INV_NROW),
                                             vtx, v0,
                                             vectorDummy, 0),
                                       q, vtx, vectorDummy),
                               $Q_VEC_SCHEMA),                                       
                   $Q_VEC)"

#
# do power-method iterations to find the eigenvector
#
CHECK_ITERS=3 # after every X (>=1) iterations, check for convergence
DIFF_COUNT=999999 # arbitrary initial value
for (( II=0; II < ITER_LIMIT; II++)) ; do
    # compute Q_k+1 = alpha H Q_k + alpha/N COL (Q_k+C_k) where edges(H)) + (1-alpha)/n M(1) Q_k where edges(H))
    # "where edges(H) is computed by merging(Q_k, edges(H)), which then pick up the necessary terms

    # the second term above is the "trapped" (or sunk) weight which must be summed and re-distributed
    # to the other active vertices.  this is the sum of weights of all "sink vertices"
    # note: V1 dot V2 = spgemm(transpose(V1), V2) [transpose can also be done by redimension]
    TRAPPED="PAGERANK_TRAPPED_$$"
    $IQUERY -aq "remove($TRAPPED)" 2>/dev/null # failure expected the first time
    timequery_n "store(spgemm(transpose($Q_VEC), $ZERO_COLS_ACTIVE_VEC ${TEST_STR}),$TRAPPED)"
    TRAP_VAL=`getValue "project(aggregate($TRAPPED,max(multiply),vectorDummy2),multiply_max)"`
    #echo "TRAP_VAL is $TRAP_VAL"

    # the third term is the "telport weight" which must be summed and re-distributed
    # it is (1-alpha) * E * P_k.  This is also called the damping factor
    TELEPORT="PAGERANK_TELEPORT_$$"
    $IQUERY -aq "remove($TELEPORT)" 2>/dev/null # failure expected the first time
    timequery_n "store(spgemm(transpose($Q_VEC), $ZERO_COLS_ACTIVE_VEC),$TELEPORT)"
    TELEPORT_VAL=`getValue "project(aggregate($Q_VEC,sum(q),vectorDummy),q_sum)"`

    # note that after adding $BETA__ROW*Q.q, we must also merge it in since the join was a sparse-vector
    # TODO: add an explicit sparse-add() operator to avoid the extra pass over the data
    timequery_n "store(cast(project(merge(project(apply(join(spgemm($MAT_IN, $Q_VEC),
                                                             $Q_VEC),
                                                        qtmp, float($ALPHA)*multiply +
                                                              float($ALPHA__NACTIVE) * float($TRAP_VAL) +
                                                              float($BETA__NACTIVE) * float($TELEPORT_VAL)),
                                                  qtmp),
                                          project(apply($Q_VEC,
                                                        qtmp, float($ALPHA__NACTIVE)*float($TRAP_VAL)+
                                                              float($BETA__NACTIVE)*float($TELEPORT_VAL)),
                                                  qtmp)),
                                    qtmp), 
                            $Q_VEC_SCHEMA),
                        $Q_VEC_NEXT)"
    STATUS=$?
    if [ "$STATUS" -ne "0" ] ; then
        echo "error in pagerank iteration $II" >&2
        exit $STATUS
    fi

    Q_VEC_NEXT_COUNT=`getCount "$Q_VEC_NEXT"`  # how many are not converged
    if [[ -n $Q_VEC_COUNT && II%CHECK_ITERS -eq 0 ]] ; then
        # TODO -- probably can remove this section due to merge in above
        if [[ "$Q_VEC_COUNT" -ne "$Q_VEC_NEXT_COUNT" ]] ; then
            # fast check, can't be equal, so just say a lot (999999) are different
            # TODO: this may not occurr anymore
            echo "setting 99999" # NCOHECKIN
            DIFF_COUNT=999999
        else
            # same size, could be equal now
            # change here means by more than $EPSILON (as a ratio of (old-new)/new)
            QUERY="filter(apply(join($Q_VEC as prior,$Q_VEC_NEXT as next),pct,abs((next.q-prior.q)/next.q)),pct>$EPSILON)"
            DIFF_COUNT=`getCount "$QUERY"`  # how many are not converged
        fi
        if [[ DIFF_COUNT -le 0 ]] ; then
            echo "@@@ at iteration $II, converged to factor of EPSILON=$EPSILON. Stopping."
        fi
    fi

    # prepare for next iteration
    $IQUERY -naq "remove($Q_VEC)"
    $IQUERY -naq "rename($Q_VEC_NEXT, $Q_VEC)"
    Q_VEC_COUNT=$Q_VEC_NEXT_COUNT # reassign count when renaming

    if [[ DIFF_COUNT -le 0 ]] ; then
        break;
    fi
done

echo "@@@ II is $II/$ITER_LIMIT, DIFF_COUNT is $DIFF_COUNT"

$IQUERY -naq "remove($ACTIVE_H)"
$IQUERY -naq "remove($ACTIVE_H_VEC)"
$IQUERY -naq "remove($NZ_COLS)"
$IQUERY -naq "remove($NZ_COLS_VEC)"
$IQUERY -naq "remove($TELEPORT)"
$IQUERY -naq "remove($TRAPPED)"
$IQUERY -naq "remove($ZERO_COLS_ACTIVE_VEC)"
$IQUERY -naq "remove($ZERO_COLS_ACTIVE)"

$IQUERY -naq "rename($Q_VEC, $RESULTNAME)"


