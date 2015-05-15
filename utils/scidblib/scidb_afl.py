#!/usr/bin/env python

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

import sys
import os
import subprocess
import math
import argparse
import datetime
import re
import traceback
import copy
import csv
from StringIO import StringIO
import scidblib

def get_iquery_cmd(args = None, base_iquery_cmd = 'iquery -o dcsv'):
    """Change iquery_cmd to be base_iquery_cmd followed by optional parameters host and/or port from args.

    @param args      argparse arguments that may include host and port.
    @param base_iquery_cmd the iquery command without host or port.
    @return the iquery command which starts and ends with a whitespace.
    """
    iquery_cmd = ' ' + base_iquery_cmd + ' '
    if args and args.host:
        iquery_cmd += '-c ' + args.host + ' '
    if args and args.port:
        iquery_cmd += '-p ' + args.port + ' '
    return iquery_cmd

def execute_it_return_out_err(cmd):
    """Execute one command, and return the data of STDOUT and STDERR.

    @param cmd   the system command to execute.
    @return a tuple (stdoutdata, stderrdata)
    @note It is up to the caller to decide whether to throw.
    """
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    return p.communicate()

def verbose_afl_result_line_start(want_re=False):
    """The beginning of a result line, if afl() or time_afl() is called with verbose=True.
    
    @param want_re  whether a regular expression is needed.
    @return the line start string (if want_re=False), or pattern (if want_re=True)
    """
    if want_re:
        return r'^\s\s---\sExecuted\s'
    else:
        return '  --- Executed '
 
def afl(iquery_cmd, query, want_output=False, tolerate_error=False, verbose=False):
    """Execute an AFL query.

    @param iquery_cmd     the iquery command.
    @param query          the AFL query.
    @param want_output    requesting iquery to output query result.
    @param tolerate_error whether to keep silent when STDERR is not empty.
                          A use case is when trying to delete an array which may or may not exist.
    @return (stdout_data, stderr_data)
    @exception AppError if STDERR is not empty and the caller says tolerate_error=False.
    """
    full_command = iquery_cmd + ' -'
    if not want_output:
        full_command += 'n'
    full_command += "aq \"" + query + "\""
    out_data, err_data = execute_it_return_out_err(full_command)
    if not tolerate_error and len(err_data)>0:
        raise scidblib.AppError('The AFL query, ' + query + ', failed with the following error:\n' +
                        err_data)
    if verbose:
        print verbose_afl_result_line_start() + '%s.' % query
    return (out_data, err_data)

def time_afl(iquery_cmd, query, verbose=False):
    """Execute an AFL query, and return the execution time.

    @param iquery_cmd the iquery command.
    @param query  the AFL query.
    @return the execution time.
    @exception AppError if the error did not execute successfully.
    """
    full_command = '/usr/bin/time -f \"%e\" ' + iquery_cmd + ' -naq \"' + query + "\" 1>/dev/null"
    out_data, err_data = execute_it_return_out_err(full_command)
    try:
        t = float(err_data)
        if verbose:
            print verbose_afl_result_line_start() + '%s in %f seconds.' % (query, t)
        return t
    except ValueError:
        raise scidblib.AppError('Timing the AFL query ' + query + ', failed with the following error:\n' +
                        err_data)

def single_cell_afl(iquery_cmd, query, num_attrs):
    """Execute an AFL query that is supposed to return a single cell, and return the attribute values.

    The return type is either a scalar (if num_attrs=1), or a list (if num_attrs>1).
    @example
      - scaler_result1 = single_cell_afl(iquery_cmd, cmd, 1)
      - scaler_result1, scaler_result2 = single_cell_afl(iquery_cmd, cmd, 2)

    @param iquery_cmd the iquery command
    @param query the query.
    @param num_attrs the expected number of attributes in the return array.
    @return the attribute value (if num_attrs=1), or a list of attribute values (if num_attrs>1)
    @exception AssertionError if num_attrs is not a positive integer.
    @exception AppError if either the query fails, or the query result is not single cell,
                     or the actual number of attributes is not num_attrs.
    """
    assert isinstance(num_attrs, (int, long)) and num_attrs>0, \
        'AssertionError: single_cell_afl must be called with a positive num_attrs.'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    lines = out_data.strip().split('\n')
    if len(lines) != 2:
        raise scidblib.AppError('The afl query, ' + query + ', is supposed to return two lines including header; but it returned ' +
                        str(len(lines)) + ' lines.')

    class DcsvDialect(csv.excel):
        """Dialect slightly tweaked from csv.excel, as a parameter to csv.reader."""
        def __init__(self):
            csv.excel.__init__(self)
            self.quotechar = "'"
            self.lineterminator = '\n'

    re_result = r'^\{0\}\s([^\n]+)$'  # A single-cell afl query returns result at row 0.
    match_result = re.match(re_result, lines[1], re.M|re.I)
    if not match_result:
        raise scidblib.AppError('The afl query, ' + query + ', did not generate ' + str(num_attrs) + ' attributes as expected.')

    string_io = StringIO(match_result.group(1))
    csv_reader = csv.reader(string_io, DcsvDialect())
    row = csv_reader.next()
    if len(row) != num_attrs:
        raise scidblib.AppError('The afl query, ' + query + ', did not generate ' + str(num_attrs) + ' attributes as expected.')
    if num_attrs==1:
        return row[0]
    return row

def get_num_instances(iquery_cmd = None):
    """Get the number of SciDB instances.

    @param iquery_cmd  the iquery command to use.
    @return the number of SciDB instances acquired by AFL query list('instances')
    @exception AppError if SciDB is not running or if #instances <= 0 (for whatever reason)
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()
    query = 'list(\'instances\')'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    num_lines = len(out_data.strip().split('\n'))
    if num_lines < 2:
        raise scidblib.AppError(query + ' is expected to return at least two lines.')
    return num_lines - 1  # skip the header line

def get_array_names(iquery_cmd = None, temp_only = False):
    """Get a list of array names.

    @param iquery_cmd  the iquery command to use.
    @param temp_only   only get the names of temp arrays.
    @return a list of array names that are in SciDB, returned by AFL query project(list(), name).
    @exception AppError if SciDB is not running or if the AFL query failed.
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()
    query = 'project(filter(list(), temporary=true), name)' if temp_only else 'project(list(), name)'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    lines = out_data.strip().splitlines()
    if not lines:
        raise scidblib.AppError(query + ' is expected to return at least one line.')
    ret = []
    for line in lines[1:]:  # Skip the header line.
        re_name = r'^\{\d+\}\s\'(.+)\'$'  # e.g.: {4} 'MyArray'
        match_name = re.match(re_name, line)
        if not match_name:
            raise scidblib.AppError('I don\'t understand the result line ' + str(i+1) + ': ' + line)
        ret.append(match_name.group(1))
    return ret
