#!/usr/bin/python

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


"""Tiny unit test, to test individual python module files."""

import sys
import re

import scidblib
from scidblib import scidb_math
from scidblib import scidb_afl
from scidblib import scidb_progress
from scidblib import statistics

def test_scidb_math_module():
    """Testing all public methods in scidblib.scidb_math."""
    print '*** testing scidblib.scidb_math...'

    a = scidb_math.comma_separated_number(1234.1234)
    assert a == '1,234.1234'
    print 'comma-separate_number(1234.1234) =', a

    a = scidb_math.fraction_if_less_than_one(0.125)
    assert a == '1/8'
    print 'fraction_if_less_than_one(0.125) =', a

    a = scidb_math.ceil_of_division(8, 3)
    assert a == 3
    print 'ceil_of_division(8, 3) =', a

    a = scidb_math.round_up(3248, 2)
    assert a == 3300
    print 'round_up(3248, 2) =', a

    a = scidb_math.round_down(3248, 2)
    assert a == 3200
    print 'round_down(3248, 2) =', a

    a = scidb_math.snap_to_grid(3161, 0.01, use_binary=False)
    assert a == 3160
    print 'snap_to_grid(3161, 0.01, use_binary=False) =', a

    a = scidb_math.snap_to_grid(3161, 0.1, use_binary=False)
    assert a == 3000
    print 'snap_to_grid(3161, 0.1, use_binary=False) =', a

    a = scidb_math.snap_to_grid(1021, 0.01, use_binary=True)
    assert a == 1024
    print 'snap_to_grid(1021, 0.01, use_binary=True) =', a

    a = scidb_math.geomean([3, 3, 4, 8])
    assert round(a, 10) == 4.1195342878
    print 'geomean([3, 3, 4, 8]) =', a
    print


def test_scidb_afl_module():
    """Testing all public methods in scidblib.scidb_afl."""
    print '*** testing scidblib.scidb_afl...'
    class TmpArgs:
        def __init__(self):
            self.host = ''
            self.port = ''

    args = TmpArgs()
    iquery_cmd = scidb_afl.get_iquery_cmd(args)
    scidb_afl.execute_it_return_out_err('ls')
    scidb_afl.afl(iquery_cmd, 'list()')

    print 'time_afl(..., \'list()\') =', scidb_afl.time_afl(iquery_cmd, 'list()')

    print 'single_cell_afl(..., \'build(<v:int64>[i=0:0,1,0], 5)\', 1) =', \
        scidb_afl.single_cell_afl(iquery_cmd, 'build(<v:int64>[i=0:0,1,0], 5)', 1)

    print 'single_cell_afl(..., \'apply(build(<v:int64>[i=0:0,1,0], 5), v2, 6)\', 2) =', \
        scidb_afl.single_cell_afl(iquery_cmd, 'apply(build(<v:int64>[i=0:0,1,0], 5), v2, 6)', 2)

    print 'get_num_instances(...) =', scidb_afl.get_num_instances(iquery_cmd)
    print 'get_array_names(...) =', scidb_afl.get_array_names(iquery_cmd)
    print

def test_scidb_progress_module():
    """Testing all public methods in scidblib.scidb_progress."""
    print '*** testing scidblib.scidb_progress...'
    print 'datetime_as_str =', scidb_progress.datetime_as_str()

    s = 'SciDB Graph500 perf-test: 14.6.7610 (2014-6-14)'
    my_match = re.match(r'^.*(' + scidb_progress.VersionAndDate.re_whole + r')', s)
    assert my_match
    mid = scidb_progress.VersionAndDate(my_match.group(1))
    smallest = scidb_progress.VersionAndDate(my_match.group(1))
    smallest.major = 13
    small = scidb_progress.VersionAndDate(my_match.group(1))
    small.revision = 7608
    large = scidb_progress.VersionAndDate(my_match.group(1))
    large.minor = 9
    s = small.__str__()
    assert smallest.earlier_than(small)
    assert small.earlier_than(mid)
    assert mid.earlier_than(large)
    assert smallest.earlier_than(large)
    print 'VersionAndDate passed unit test.'

    pt = scidb_progress.ProgressTracker()
    pt.register_step('one', 'the first step')
    pt.start_step('one')
    pt.end_step('one')
    print

def test_statistics_module():
    """Testing all public methods in scidblib.statistics."""
    print '*** testing scidblib.statistics...'
    data = [3, 3, 4, 8]

    a = statistics.pstdev(data)
    assert round(a, 10) == 2.0615528128
    print 'pstdev =', a

    a = statistics.pvariance(data)
    assert a == 4.25
    print 'pvariance =', a

    a = statistics.stdev(data)
    assert round(a, 10) == 2.3804761428
    print 'stdev =', a

    a = statistics.variance(data)
    assert round(a, 10) == 5.6666666667
    print 'variance =', a

    a = statistics.median(data)
    assert a == 3.5
    print 'median =', a

    a = statistics.median_low(data)
    assert a == 3
    print 'median_low =', a

    a = statistics.median_high(data)
    assert a == 4
    print 'median_high =', a

    a = statistics.median_grouped(data)
    assert a == 3.5
    print 'median_grouped =', a

    a = statistics.mean(data)
    assert a == 4.5
    print 'mean =', a

    a = statistics.mode(data)
    assert a == 3
    print 'mode =', a
    print

def main():
    test_scidb_math_module()
    test_scidb_afl_module()
    test_scidb_progress_module()
    test_statistics_module()
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN

