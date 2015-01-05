#!/bin/sh
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
set -eux
function do_restart()
{
./runN.py 1 scidb --istart
sleep 1
}

R_PATH=testcases/r/perf/repart
T_PATH=testcases/t/perf/repart

function do_test()
{
    rm -rf ${R_PATH}/*
    for kind in sparse dense; do
	for name in `(cd ${T_PATH}; ls *.test | grep $kind) | sed -e "s/\.test//g"`; do 
	    echo "Mode: $1 Test: $name"
	    do_restart
	    ../../bin/scidbtestharness --root-dir=testcases/ --test-id=perf.repart.$name --record
	done;
    done;
    rm -rf $1
    mkdir -p $1
    for name in `find ${R_PATH} -name "*.timer"`; do cp $name $1; done;
}
unset REPART_ENABLE_TILE_MODE
unset TILE_SIZE
echo "Value mode"
do_test "value"
export REPART_ENABLE_TILE_MODE=1
unset TILE_SIZE
echo "Tile mode"
do_test "tile"
