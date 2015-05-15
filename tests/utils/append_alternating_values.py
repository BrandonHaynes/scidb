#!/usr/bin/python
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2014 SciDB, Inc.
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
"""This script appends values to the end of all chunks of an array.

@author Donghui Zhang

@note Assumptions:
  - iquery is in your PATH.
  - <SCIDB_INSTALL_PATH>/bin or <SCIDB_TRUNK>/utils is in your PYTHONPATH.
  - Your SciDB revision is 7926 or later.

@note This script can reproduce the bugs in http://trac.scidb.net/ticket/4300 as follows:
  - Checkout SciDB source code with revision between [7926..7932].
    Explanation: the script uses the op_set_cell_attr_1D macro committed in changeset:7926;
    The SCIDB_LE_DATASTORE_CHUNK_CORRUPTED bug is fixd in changeset:7933.
  - Modify entry.cpp to call initSharedMemCache with 2*KiB, i.e. reducing the size of
    the SharedMemCache, to force flushing.
  - Make and install a single-instance Debug build of SciDB.
  - Place this script in either <SCIDB_INSTALL_PATH>/bin or <SCIDB_TRUNK>/utils.
    Alternatively, include one of those in PYTHONPATH.
  - Run the following command to produce a SCIDB_LE_DATASTORE_CHUNK_CORRUPTED exception,
    and immediately run again to produce a SCIDB_LE_CANT_SEND_RECEIVE crash:
    append_alternating_values.py -v

@note This script may be used to benchmark small updates with the goal to speedup
  temp arrays and MemArray. See http://trac.scidb.net/ticket/4303.
"""

#
# Whether to print traceback, upon an exception
#
_print_traceback_upon_exception = True

import sys
import os
import subprocess
import math
import argparse
import datetime
import re
import traceback
import copy

import scidblib
from scidblib import scidb_afl
from scidblib import scidb_progress

array_name =        "array_append_alternating_values"
ranges_array_name = "ranges_array_append_alternating_values"

def my_remove_arrays(iquery_cmd, tolerate_error):
    cmd = "remove(%s)" % (array_name);
    scidb_afl.afl(iquery_cmd, cmd, tolerate_error=tolerate_error)
    cmd = "remove(%s)" % (ranges_array_name);
    scidb_afl.afl(iquery_cmd, cmd, tolerate_error=tolerate_error)

def my_test(args, num_chunks, chunk_length, initial_values_per_chunk, new_values_per_chunk, type_name):
    """This function does the testing of appending alternate values to the end of every chunk of an array.

    @param args                          command-line parameters.
    @param num_chunks                    how many chunks are there.
    @param chunk_length                  the chunk length.
    @param initial_values_per_chunk  the number of initial values per chunk
    @param new_values_per_chunk      how many value to insert into each chunk.
    @param type_name                     the data type.
    @return 0
    """
    # Set even_value and odd_value.
    even_value = "0"
    odd_value = "1"
    if type_name=="bool":
        even_value = "true"
        odd_value = "false"

    # Initialize the ProgressTracker
    progress_tracker = scidb_progress.ProgressTracker(if_print_start = args.verbose, if_print_end = args.verbose)
    progress_tracker.register_step('initial', 'Load initial values.')
    progress_tracker.register_step('new', 'Insert new values.')

    # Remove the array if exists.
    iquery_cmd = scidb_afl.get_iquery_cmd(args)
    my_remove_arrays(iquery_cmd, tolerate_error=True)

    # Create the array.
    cmd = "create temp array %s <v:%s>[i=0:%d,%d,0]" % (array_name, type_name, chunk_length*num_chunks-1, chunk_length)
    scidb_afl.afl(iquery_cmd, cmd)

    # Load initial values.
    # The algorithm is to create an array that describes the ranges for the initial values,
    # then use cross_between to filter out values from a fully-populated array.
    progress_tracker.start_step('initial')
    cmd = "create temp array %s <low:int64, high:int64>[i=0:%d,%d,0]" % (ranges_array_name, num_chunks-1, num_chunks)
    scidb_afl.afl(iquery_cmd, cmd)
    for c in xrange(num_chunks):
        cmd = ("insert(redimension(apply(build(<adummyattribute:bool>[adummydim=0:0,1,0],true), i, %d, low, %d, high, %d), %s), %s)"
              % (c, c*chunk_length, c*chunk_length+initial_values_per_chunk-1, ranges_array_name, ranges_array_name))
        scidb_afl.afl(iquery_cmd, cmd)
    cmd = ("store(cross_between(build(%s, iif(i%%2=0, %s(%s), %s(%s))), %s), %s)"
           % (array_name, type_name, even_value, type_name, odd_value, ranges_array_name, array_name))
    scidb_afl.afl(iquery_cmd, cmd)
    progress_tracker.end_step('initial')

    # Load the additional values.
    progress_tracker.start_step('new')
    if args.verbose:
        print "In each of the %d batches, one value will be appended to each of the %d chunks." % (new_values_per_chunk, num_chunks)
        print "Batch\tTime"
    for i in xrange(new_values_per_chunk):
        start_time = datetime.datetime.now()
        for c in xrange(num_chunks):
            index = c*chunk_length+i+initial_values_per_chunk
            value = type_name+"("+even_value+")" if index%2==0 else type_name+"("+odd_value+")"
            cmd = "op_set_cell_attr_1D(%s, i, %d, v, %s)" % (array_name, index, value)
            scidb_afl.afl(iquery_cmd, cmd)
        if args.verbose:
            seconds = scidb_progress.timedelta_total_seconds(datetime.datetime.now() - start_time)
            print "%d\t%f" % (i+1, seconds)
    progress_tracker.end_step('new')

    # Remove the array.
    my_remove_arrays(iquery_cmd, tolerate_error=False)

    # Return 0
    return 0

def main():
    """The main function gets command-line arguments and calls calculate_chunk_length().

    @return 0
    @exception AppError if something goes wrong.
    @note If print_traceback_upon_exception (defined at the top of the script) is True,
          stack trace will be printed. This is helpful during debugging.
    """
    parser = argparse.ArgumentParser(
                                     description='Append values, that alternate for odd and even positions, to every chunk.\n' +
                                     '\n' +
                                     'The program creates an array, add values gradually to all chunks, then removes the array.',
                                     epilog=
                                     'assumptions:\n' +
                                     '  - iquery is in your PATH.\n' +
                                     '  - <SCIDB_INSTALL_PATH>/bin or <SCIDB_TRUNK>/utils is in your PYTHONPATH.\n' +
                                     '  - Your SciDB revision is 7926 or later.',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--host',
                        help='Host name to be passed to iquery.')
    parser.add_argument('-p', '--port',
                        help='Port number to be passed to iquery.')
    parser.add_argument('-n', '--num-chunks', type=int, default=10,
                        help='''The number of chunks.
                        The default value is 10.'''
                        )
    parser.add_argument('-l', '--chunk-length', type=int, default=500,
                        help='''The chunk length. Default is 500.'''
                        )
    parser.add_argument('-i', '--initial-values-per-chunk', type=int, default=420,
                        help='''The number of values per chunk.
                        The default is 420.'''
                        )
    parser.add_argument('-d', '--new-values-per-chunk', type=int, default=10,
                        help='''The number of values per chunk.
                        The default is 10.'''
                        )
    parser.add_argument('-t', '--type-name', choices=['int64', 'int32', 'int16', 'int8', 'bool'], default='int8',
                        help='''The data type. The default is int8.''')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='If specified, progress of the algorithm will be reported.')
    args = parser.parse_args()

    try:
        num_chunks = args.num_chunks
        chunk_length = args.chunk_length
        initial_values_per_chunk = args.initial_values_per_chunk
        new_values_per_chunk = args.new_values_per_chunk
        type_name = args.type_name
        if initial_values_per_chunk+new_values_per_chunk > chunk_length:
            print "Argument error: initial_values_per_chunk + new_values_per_chunk cannot exceed chunk_length."
            sys.exit(1)
        exit_code = my_test(args, num_chunks, chunk_length, initial_values_per_chunk, new_values_per_chunk, type_name)
        print "Testing completed successfully."
        assert exit_code==0, 'AssertionError: the command is expected to return 0 unless an exception was thrown.'
    except Exception, e:
        print >> sys.stderr, '------ Exception -----------------------------'
        print >> sys.stderr, e

        if _print_traceback_upon_exception:
            print >> sys.stderr, '------ Traceback (for debug purpose) ---------'
            traceback.print_exc()

        print >> sys.stderr, '----------------------------------------------'
        sys.exit(-1)  # upon an exception, throw -1

    # normal exit path
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
