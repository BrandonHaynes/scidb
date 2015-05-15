#!/bin/bash

set -eu

function usage()
{
    echo "Usage: $0 <result dir>"
    exit 1
}

function die()
{
    echo $1
    exit 1
}


[ ! "$#" -eq 1 ] && usage

pushd boost
./build.sh $1
popd

pushd libcsv
./build.sh $1
popd

pushd mpich2
./build.sh $1
popd
