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


# defaults if not given on command line
ORDER_MIN=2
ORDER_MAX_VERBOSE=8
ORDER_MAX=8
ORDER_STEP_TYPE="x" # or "+"
ORDER_STEP=2


if [ -z "$SCRIPT_DIR" ] ; then
    SCRIPT_DIR="src/linear_algebra"
fi

if [ "$1" != "" ] ; then
    ORDER_MIN=$1  # TODO: replace this with getopt
    ORDER_MAX_VERBOSE=$1
    ORDER_MAX=$1
fi

if [ "$2" != "" ] ; then
    ORDER_MAX_VERBOSE=$2
    ORDER_MAX=$2
fi

if [ "$3" != "" ] ; then
    ORDER_MAX=$3
fi

if [ "$4" != "" ] ; then
    ORDER_STEP_TYPE=$4
fi

if [ "$5" != "" ] ; then
    ORDER_STEP=$5
fi



echo "$0: ====================================="
echo "$0: $0 $ORDER_MIN,$ORDER_MAX_VERBOSE,$ORDER_MAX begin"


ORDER=$ORDER_MIN
while [ "$ORDER" -le "$ORDER_MAX_VERBOSE" ] ; do

    if [ "${ORDER_STEP_TYPE}" = "+" ] ; then
        ORDER=`expr "$ORDER" '+' "$ORDER_STEP"`
    elif [ "${ORDER_STEP_TYPE}" = "x" ] ; then
        ORDER=`expr "$ORDER" '*' "$ORDER_STEP"`
    else
        echo "$0: illegal value for ORDER_STEP_TYPE, = ${ORDER_STEP_TYPE}"
        exit 5
    fi
done


while [ "$ORDER" -le "$ORDER_MAX" ] ; do
    PROC_ORDER=$(( (ORDER + 31)/32 ))
    PROC_ORDER=$(( PROC_ORDER <= 8 ? PROC_ORDER : 8 ))
    NPROC=$(( PROC_ORDER * PROC_ORDER ))
    CMD="mpirun.mpich2 -np $NPROC plugins/mpi_slave_direct $ORDER $PROC_ORDER $PROC_ORDER"   # NOCHECKIN
    echo "-------------------------------------"
    echo $CMD
    /usr/bin/time -f'%E s elapsed' $CMD
    echo


    if [ "${ORDER_STEP_TYPE}" = "+" ] ; then
        ORDER=`expr "$ORDER" '+' "$ORDER_STEP"`
    elif [ "${ORDER_STEP_TYPE}" = "x" ] ; then
        ORDER=`expr "$ORDER" '*' "$ORDER_STEP"`
    else
        echo "$0: illegal value for ORDER_STEP_TYPE, = ${ORDER_STEP_TYPE}"
        exit 5
    fi
done

echo "$0: $0 $ORDER_MIN,$ORDER_MAX_VERBOSE,$ORDER_MAX end"
echo 

