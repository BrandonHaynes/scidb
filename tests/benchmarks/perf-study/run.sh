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
#
#    File:   run.sh 
#
#   About: 
#
#   This script orchestrates the array creation and query workloads that make 
#  up this performance benchmark. To run successfully, it requires that you 
#  configure a set of SciDB server installations in the config.ini file that 
#  have the Installation_Base_Name, and then a scale number. So if you wanted 
#  to use an Installation_Base_Name like "$Installation_Base_Name", and 4 scale factors, then 
#  you would create $Installation_Base_Name1, $Installation_Base_Name2, $Installation_Base_Name4, and $Installation_Base_Name8
#  
#   Usage: ./run.sh Chunk_Count Chunk_Length Installation_Base_Name 
#
# set -x 
#
#
usage()
{
  echo "Usage: run.sh Chunk_Count Chunk_Length Installation_Base_Name Port"
  echo " Chunk_Count and Chunk_Length must be integers > 0"
  echo " Installation_Base_Name must be a string. Used as a base label"
  echo "to specify which scale of installation to use. For example, "
  echo "a base label of 'foo' is used to scale through foo1, foo2, etc"
  echo "and each base-label.scale-factor must be a installation in the "
  echo "config.ini file."
  echo " Port must be a the SciDB coordinate server TCP/IP port number."
  echo "It is the 'base-port' configuration from the config.ini for the"
  echo "server."
  exit;
}
#
#  Check that the environment variable used to discover the location of the 
#  storage is set. 
#
if [ "$SCIDB_DATA_DIR" = "" ]; then
	echo "You must set the variable \$SCIDB_DATA_DIR to the directory in which the "
	echo "SciDB data is stored. Use the the 'base-path' configuration from the "
	echo "config.ini file."
	exit;
fi
#
#  Check for the right number of command line arguments, and that the 
#  command line arguments make sense. 
#
if [ $# -ne 4 ]; then 
	usage;
fi
#
#   The Chunk_Length and Chunk_Count here are define the unit of scale in the 
#  benchmark. We start with this size as what you want to run on a single 
#  instance installation. Then when you double the number of instances, we 
#  double the size of the data involved (although this is a bit tricky, as we
#  shall see below where we make the calculations) because we want to keep the 
#  chunk-size (chunk_length x chunk_length) as close to original as possible. 
#
Chunk_Count=$1
Chunk_Length=$2
Installation_Base_Name=$3
Port=$4
#  
if [ $Chunk_Count -le 0 ]; then
    echo "Chunk_Count must be > 0"
    usage;
fi
#
if [ $Chunk_Length -le 0 ]; then
    echo "Chunk_Length must be > 0"
    usage;
fi
if [ $Port -le 0 ]; then
    echo "Port must be identified - look in your config.ini"
    usage;
fi
#
#   Perform a minimal check to try to ensure that the Installation_Base_Name
#  makes some kind of sense. 
CONFIG_INI="/opt/scidb/${SCIDB_VER}/etc/config.ini";
if [ -r "${CONFIG_INI}" ] ; then 
	SC=`grep ${Installation_Base_Name} ${CONFIG_INI} | wc -l`;
	if [ "$SC" -eq 0 ]; then 
		echo "Servers for ${Installation_Base_Name} not found in Config File ${CONFIG_INI}";
	fi
else 
	echo "Config File ${CONFIG_INI} not found";
fi
#
#  Grab the SciDB version number/svn revision. We prepend this value to the 
# .csv file's entries because we will build a larger and larger collection 
# of benchmark data over time. 
#  
V=`scidb --version | grep version | awk '{print $3}' | awk -F. '{print $4}'`;
#
#  The metrics we record in this benchmark are are: 
#   1. Per-query wall-clock times. 
#   2. Per-Array storage (per-instance). 
#   3. As the queries run, the memory allocated to SciDB as a % of total. 
#   4. The size of the array (as a check) 
#
#   Each of these metrics finds its way into a .csv file created during the 
#  run. The next few lines establish what the names of these files will be. 
#  Each run of the benchmark script truncates these files, so to build a 
#  long term historical log, you need to copy the files elsewhere. 
#
TIMING="/tmp/scidb_load_format_timings_$1_$2.csv"
# rm -rf $TIMING
STORAGE="/tmp/scidb_load_format_storage_$1_$2.csv"
# rm -rf $STORAGE
MEM_USAGE="/tmp/scidb_load_format_mem_usage_$1_$2.csv"
# rm -rf $MEM_USAGE
ARRAY_SIZE="/tmp/scidb_load_format_array_size_$1_$2.csv"
# rm -rf $ARRAY_SIZE
#
#  We want to exercise the Queries over the Array at multiple scale factors. 
# The list below indicates what these scale factors are. Note that the script 
# assumes that there is a SciDB instance configured for each scale factor. For 
# simplicity, on bigboy these are called $Installation_Base_Name1, $Installation_Base_Name2, $Installation_Base_Name4, and this practice
# of "instance_config:scale_factor" seems useful. 
#  
for Scale_Factor in 32 8 2; do 
#
#  Truncate the performance study log file at each scale factor. 
  echo "" > /tmp/perf-study.txt 
#
#  We compute a new chunk count, and chunk length, for each scale 
# factor. To keep the test valid, we want to keep the chunk's sizes 
# about the same as we scale things up. This leads to a little bit 
# of tricky math. 
#
#  The array is square. So the new chunk count is CC * sqrt ( Scale_Factor). 
  CC=`dc -e "0.5 ${Chunk_Count}.0 ${Scale_Factor}.0 v * + n" | sed -e "s/\..*$//g"`
#
#   Now, we want to keep chunk_length about the same also. But if we kept 
#  it precisely the same (1000) then we're going to be off on the 
#  data size calculation. So we need to figure out a new Chunk_Length 
#  also. 
#
#   The new Chunk_Length will be the Scale_Unit (Original Chunk_Count 
#  times Chunk_Length) times Scale_Factor, divided by the new Chunk_Count.
  TS=`dc -e "0.5 ${Chunk_Count}.0 ${Chunk_Length} ${Scale_Factor}.0 v * * + n" | sed -e "s/\..*$//g"`
#
#   Because of a number of restrictions in SciDB connected to operators
#  and chunk_length, we make sure here that the new Chunk_Length is 
#  divisible by 8, and will always be slightly larger than the 
#  original Chunk_Length (though not by much). 
  CL=`dc -e "${TS}.0 ${CC}.0 / d 8 % 8 - -1 * n" | sed -e "s/\..*$//g"`
#
#   Report what has been calculated to the user can eyeball the values and 
#  monitor the progress of the benchmark. These values don't find their way 
#  into the log at all, except as part of the queries being reported. 
  echo "Scale_Factor = $Scale_Factor, so Target Size = $TS, Chunk Count = $CC and Chunk Length = $CL "
#
#   The installation we use is determined here. The idea is to completely 
#  re-init SciDB each time in an effort to avoid any resource issues we might 
#  get.
#
#   NOTE: The $Installation_Base_Name1, $Installation_Base_Name2 and $Installation_Base_Name4 installations are in the config.ini on 
#  bigboy.local.paradigm4.com. If you want to move this script to another // 
#  platform, you'll need to adjust the way the installation is choosen at 
#  each scale factor. 
#
#   Note also that, each time an Array is created, the scidb installation is 
#  re-initialized. This has the potential to cause the storage space to grow, 
#  as we keep copies of the old storage each time we re-initialize it. There
#  is a config.ini option to limit the number of copies we keep. 
  Inst="$Installation_Base_Name${Scale_Factor}"
#
#   WE investigate 3 levels of sparseness; dense, 1% and 0.01%. 
  for Sparseness in 1.0 0.01 0.0001; do
#
#   We investigate 3 degrees of "frequency distribution". The idea is to check 
#  the system against uniform distributions, less-than- uniform, and 
#  distributions where the distinct count of values is very low. (What's the 
#  storage effect of RLE encoding?)
#
    for Zipf in 1.0 0.5 0.1; do
#
#   For each utilization of Arrays.sh and Queries.sh, print what you're about 
#  to do. 
	echo "./Arrays.sh $CC $CL $Sparseness $Zipf $Inst $Port"
	./Arrays.sh $CC $CL $Sparseness $Zipf $Inst $Port >> /tmp/perf-study.txt 2>&1
	echo "./Queries.sh $CC $CL $Port"
    ./Queries.sh $CC $CL $Port >> /tmp/perf-study.txt 2>&1
#
#   Stop the instance here, if it is running. We re-initialize it in Arrays.sh 

scidb.py stopall $Inst

    done;
#
#  As we increase the sparseness, we need to adjust the chunk_length as well, 
# so as to ensure that each run addresses the same amount of data (same number 
# of non-empty cells). As the array is square, an increase in the sparseness 
# from 100% to 1% requires a 100x increase in the *logical* size of the array, 
# which means a 10x increase in the length of the chunks in each dimension. 
    CL=`expr $CL "*" 10`
  done;
#
#  At this point, we have gone through a complete run at a given scale factor. 
# So we need to extract the information we're looking for from the log file,
# and format it into .csv lines together with the scidb version and the 
# scale factor information.
  grep "^Q[1-9]" /tmp/perf-study.txt | grep -v Query | sed -e "s/Q//g" | awk 'BEGIN {q="\x22"; v="'$V'"; s="'$Scale_Factor'"} {printf("%d,%d,%d,%d,%g\n", v, s, ((NR-1)/18)+1, $1, $2)}' >> $TIMING

#
#  Extract the per-array storage size information from the log, and format it 
# into a .csv file, together with SciDB version, and Scale Factor. 
  grep "storage.data1" /tmp/perf-study.txt | grep -v "du" | sed -e "s/G.*$//g" | awk 'BEGIN {q="\x22"; v="'$V'"; s="'$Scale_Factor'"} {printf("%d,%d,%cA%d%c,%g\n", v, s, q, NR, q, $1)}' >> $STORAGE

#
#  Extract the per-array size information from the log, and format it into 
# a .csv file, together with SciDB version, and Scale Factor. 
  grep "\"Size_Count" /tmp/perf-study.txt | awk -F , 'BEGIN {q="\x22"; v="'$V'"; s="'$Scale_Factor'"} {printf("%d,%d,%cA%d%c,%d\n", v, s, q, NR, q, $3)}' >> $ARRAY_SIZE

# 
#  NOTE: Memory usage is a bit problematic. For the moment, don't gather the 
#        per-instance memory usage. I need to check that I can combine the 
#        memory for the guard process with the memory for the engine process.
#        At this point, there doesn't seem to be a problem doing that. 
#  echo "./scidb_load_format_memory.sh $V $Scale_Factor /tmp/perf-study.txt >> $MEM_USAGE"
#   ./scidb_load_format_memory.sh $V $Scale_Factor /tmp/perf-study.txt >> $MEM_USAGE;

#
# Copy the results of the run into a new file. 
cp /tmp/perf-study.txt /tmp/perf-study-${Scale_Factor}.txt 

done;
#
echo "==-- DONE --=="
#
exit
