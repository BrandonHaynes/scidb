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


echo "this is not make-based.  calling this script forces a clean rebuild"
echo "of scalapack 2.0.2 in a subdirectory we will create/overwrite "
echo "in the current directory"
echo "you have 5 seconds to ^C this script if you do not wish to overwrite"
echo "your current build of scalapack 2.0.2  via the installer 1.0.2"

for S in 5 4 3 2 1 0 ; do
   echo $S
   sleep 1
done

TOP_DIR=scalapack_installer/1.0.2
mkdir -p $TOP_DIR
pushd $TOP_DIR

echo "okay, proceeding"

if [ ! -f scalapack_installer.tgz ] ; then
    echo "you don't have the tar file, getting it from netlib"
    wget http://www.netlib.org/scalapack/scalapack_installer.tgz
fi

if [ ! -f scalapack_installer.tgz ] ; then
    echo "error getting tar file from netlib"
    popd # $TOP_DIR
    exit 1
fi

NEXT_DIR=scalapack_installer_1.0.2
rm -rf $NEXT_DIR

tar xvf scalapack_installer.tgz

if [ -f scalapack_installer_1.0.2 ]; then
    echo "error getting tar file from netlib"
    popd # $TOP_DIR
    exit 1
fi

pushd $NEXT_DIR

#
# The ccflags/fcflags/nopt(flags) below will change.
# They will be -g -O0 for now until we get furthur
# along in our debugging.  Optimizing the scalapack code does not result in
# significant speedup anyway
#
./setup.py --verbose \
     --mpibindir=/usr/bin \
     --mpicc=mpicc.mpich2 \
     --mpif90=mpif90.mpich2 \
     --mpirun=mpiexec.mpich2 \
     --ccflags="-fPIC -g -O0" \
     --fcflags="-fPIC -g -O0" \
     --noopt="-fPIC -g -O0" \
     --mpiincdir="-I/usr/include/mpich2" \
     --lapacklib "/usr/lib/liblapack.so /usr/lib/libblas.so" \
     --blaslib "/usr/lib/libblas.so"
     # do we need $FLIB too?
     # so there seems to be a bug that when it verifies lapacklib, is uses lapacklib for the blas
     # by mistake, so to fix it, you have to add the blaslilb to the lapacklib
     # --downlapack
     # --downblas
     # --notesting
     # --makecmd="make -j12"  # says not recognized switch
STATUS=$?

popd # $NEXT_DIR
popd # $TOP_DIR

exit $?
