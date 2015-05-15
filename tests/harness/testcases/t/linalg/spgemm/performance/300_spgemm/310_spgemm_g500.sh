#!/bin/bash
#

#set -x

# what vars are there?
printenv  | fgrep -i scidb  # NOCHECKIN
printenv  | fgrep DIR  # NOCHECKIN

echo CMD is $* # show the command line
echo I is $I  # argument, instances 
echo S is $S  # argument, scale


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
    iquery -aq "create array mat_scale$S <v:double>[i=0:${N}-1,${N_CAPPED}/$DIVS,0, j=0:${N}-1,${N_CAPPED}/$DIVS,0]"

    # load the list of v0,v1 edges (vertex pairs) (checked into the test/data directory)
    SS=`printf "%0*d" 2 $S` # add leading 0s to width of 2 (for filename)
    iquery -naq "load(pairs_scale$S,'/public/data/graph500/g500s$SS.dat',-2,'(int64,int64)')"
    #NOCHECKIN iquery -aq "aggregate(pairs_scale$S, count(*))" # perf validation

    # turn into adjacency matrix
    iquery -naq "store(redimension(apply(pairs_scale$S, v, 1.0), mat_scale$S),mat_scale$S)"
    #aggregate(mat_scale$S, count(v)) # perf validation, off because slow
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
    iquery -aq "consume(spgemm(mat_scale$S, mat_scale$S))"

fi

#
# cleanup
#
if [ "$SECTION" == "CLEANUP" ] ; then
    iquery -aq "remove(pairs_scale$S)" || true
    iquery -aq "remove(mat_scale$S)" || true
fi

exit 0

