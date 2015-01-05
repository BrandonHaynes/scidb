This file is out-of-date and will be removed soon.
Please see:

    http://trac.paradigm4.com/wiki/Development/Checkout

for instructions on compiling the full linear_algebra library

-----
This is a file to keep track of some issues that cause difficulty when
compling, linking, or running this code, particularly as we work through issues
with the cmake configuration, ScaLAPACK, BLACS, LINPACK, BLAS, and MPI.

Please use wiki-markup in this file, so we may easily transfer it to wiki
documentation when we merge this to trunk, as we will need to have documentation
for other developers at that time

* BUILD PREQUISITES:
 * gfortran, probably any version
 * cmake-2.8.5 (See http://trac.scidb.org/wiki/PrivateArea/Build_trunk for cmake installation instructions).
 * Ubuntu: check your version with 'lsb_release -a'
 * Ubuntu: supporting 11.4, 11.10  (< 11.4 will not be supported) 
  * sudo apt-get install libboost1.42-all-dev
  * sudo apt-get install libscalapack-mpi1 # ScaLAPACK and ATLAS BLAS
  * sudo apt-get install liblapack-dev
  * sudo apt-get install libopenmpi-dev
   * follow instructions for SSH http://www.open-mpi.org/faq/?category=rsh
   * make sure to ssh to all workers from the coordinator using the actual IP and say 'yes'
   * [optional] openmpi-doc
   * [execute-only dependency] openmpi-bin

* BUILDING from source, e.g. to debug:
 * MPI: old instructions:
  * Download it: $wget ....
  * $../configure --with-threads=posix --enable-mpi-threads --disable-dlopen --with-platform=optimized
  * $ make all
  * $ sudo make install
  * $ export LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}
  * $ export PATH=/usr/local/bin:$PATH
  * rememmber to put the above two lines in your .bashrc file or equivalent
  * THE LD_LIBRARY_PATH must be set so that CMake will find the correct MPI libraries
  * before I did that, it would not find them in /usr/local/lib
  * (Note: I also had set PATH so that 
 * SCIDB: rebuild scidb ? I'm not sure this is needed, see #461 if it is
 * P4:
  * $cmake -DCMAKE_CXX_FLAGS='-DRUN_DLA' -DSCIDB_SOURCE_DIR=`pwd`/../<scidb-trunk> -DCMAKE_BUILD_TYPE=Debug .

* RUNNING 
 * you have to run with runtests, not runN.py OR 
 * do what runtests does to make links scidb_runner -> mpirun  and (local)orted->(installed)orted
