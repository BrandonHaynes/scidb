#!/bin/bash
#
set -e # bail with error first bad command|pipeline
#set -x

#
# handy functions
#

# return the first attribute from a 1D array at location [idx]
function lookup { local aName=$1 ; local idx=$2
    local VAL=`iquery -otsv -aq "filter($aName, dummy=${idx})" | tail -n1 | cut -f1`
    echo $VAL
}


# what vars are there?
printenv  | fgrep -i scidb  # NOCHECKIN
printenv  | fgrep DIR  # NOCHECKIN

echo CMD is $* # show the command line
echo I is $I  # argument, instances 
echo S is $S  # argument, scale

REPS=${REPS:-1}
echo REPS is $REPS # how many times to repeat the timing


N=$((2**S))  # order
echo N is $N

# cap sum of chunksizes at 524288  (otherwise paging likely, reduce this number on machines <64GB)
N_CAPPED=$((N<524288?N:524288))
echo N_CAPPED is $N_CAPPED

# divide into $I chunks, when enough cells to divide
DIVS=$((N>I?I:1))
echo DIVS is $DIVS

CSIZE=$((N_CAPPED/DIVS))
echo CSIZE is $CSIZE

#
# setup
#
if [ "$SECTION" == "SETUP" ] ; then
    #
    # preliminaries
    #
    iquery -aq "load_library('linear_algebra')"

    iquery -aq "remove(pairs_scale$S)" 2> /dev/null || true
    iquery -aq "create array pairs_scale$S <i:int64, j:int64>[dummy=0:*,1000*1000,0]"

    iquery -aq "remove(mat_scale$S)" 2> /dev/null || true
    iquery -aq "create array mat_scale$S <v:float>[i=0:${N}-1,$CSIZE,0, j=0:${N}-1,$CSIZE,0]"

    # load the list of v0,v1 edges (vertex pairs) (checked into the test/data directory)
    SS=`printf "%0*d" 2 $S` # add leading 0s to width of 2 (for filename)
    iquery -naq "load(pairs_scale$S,'/public/data/graph500/g500s$SS.dat',-2,'(int64,int64)')"
    #NOCHECKIN iquery -aq "aggregate(pairs_scale$S, count(*))" # perf validation

    # turn into adjacency matrix
    iquery -naq "store(redimension(apply(pairs_scale$S, v, float(1)), mat_scale$S),mat_scale$S)"
    #aggregate(mat_scale$S, count(v)) # perf validation, off because slow

    iquery -aq "remove(vec_scale$S)" 2> /dev/null || true
    COL_SCHEMA="<v:float>[r=0:$N-1,$CSIZE,0, col=0:0,$CSIZE,0]"
    if false ; then
        # graph500 BFS initial vector:
        # fill the initial vector
        # pick some pair at random from the pairs, take one of its vertices and set it
        # since the pairs are randomized, just take one of the vertices
        VERTEX=`lookup pairs_scale$S 0`  # get first coordinate from first entry in pairs
        echo VERTEX=$VERTEX

        # NOCHECKIN without -n
        # NOCHECKIN nogo on float conversion from bool
        # iquery -aq "store(build(vec_scale$S, float(int32(i=$VERTEX))),vec_scale$S)"

        # make a "vector" (single column matrix)
        # take an array with TMP[0]=(v:float(1)), apply makes it TMP[col:0]=(v:float(1),row:$VERTEX)
        TMP="apply(build(<v:float>[col=0:0,1,0], float(1)), row, $VERTEX)"
        # and redim makes it vec_scale$S[$VERTEX,0] = float(1)
        iquery -aq "store(redimension($TMP, $COL_SCHEMA), vec_scale$S)"
    else
        # make a "vector" (single column matrix) of alternating 0's and 1's (worst case?)
        # take an array with TMP[0]=(v=float(1)), apply makes it TMP[r:0]=(v:float(1),col:0)
        TMP="apply(build(<v:float>[r=0:$N-1,$CSIZE,0], float(r%2)), col, 0)"
        # and redim makes it vec_scale$S[$VERTEX,0] = float(1)
        iquery -aq "store(redimension($TMP, $COL_SCHEMA), vec_scale$S)"
    fi
fi

#
# test
#
if [ "$SECTION" == "TEST" ] ; then
    # find NNZs in the product (does not always need to be enabled, as slow at large scale)
    # [normally disabled]
    # iquery -naq "store(spgemm(mat_scale$S, mat_scale$S), product_scale$S)"
    # iquery -aq "aggregate(product_scale$S, count(multiply))"
    # iquery -aq "remove(product_scale$S)"

    #
    # speed test of spgemm, as SpMV as used in BFS search in graph500 test
    #

    # handy test when debugging:
    # before looping, make transposes of the matrix, to check for alg preference
    # of spped for M*V vs transpose(V) * transpose(M)
    #
    #iquery -aq "remove(vec_scale_tr$S)" 2> /dev/null || true
    #iquery -aq "remove(mat_scale_tr$S)" 2> /dev/null || true
    #iquery -naq "store(transpose(vec_scale$S), vec_scale_tr$S)"
    #iquery -naq "store(transpose(mat_scale$S), mat_scale_tr$S)"
    #
    # matrix-vector is now clearly faster, so commented out

    for (( i=1; i<=$REPS; i++))
    do
        #iquery -naq         "spgemm(mat_scale$S,    vec_scale$S)"
        iquery  -aq "consume(spgemm(mat_scale$S,    vec_scale$S))"
    done
fi

#
# cleanup
#
if [ "$SECTION" == "CLEANUP" ] ; then
    iquery -aq "remove(pairs_scale$S)" || true
    iquery -aq "remove(mat_scale$S)" || true
    iquery -aq "remove(vec_scale$S)" || true
fi

exit 0

