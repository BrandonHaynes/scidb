#!/usr/bin/python
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
"""This script calculates chunk lengths of an array.
A typical use case is to consult the script to get chunk lengths,
before running the redimension() query.

@author Donghui Zhang

Assumptions:
  - It assumes SciDB is running.
  - It assumes 'iquery' is in your path.

Version history:
  - 14.7: initial released SciDB version where the script is added.
"""

#
# Whether to print traceback, upon an exception
#
_print_traceback_upon_exception = False

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
from scidblib import scidb_math
from scidblib import scidb_afl
from scidblib import scidb_progress
from scidblib import statistics

class Dimension:
    """The specification of one dimension.

    Details of public attributes:
      - dim_name: dimension name
      - dim_low: low coordinate
      - dim_high: high coordinate
      - chunk_length: chunk length
      - chunk_overlap: chunk overlap
      - min_coord: min coordinate
      - max_coord: max coordinate
      - distinct_count: distinct count of the dimension

    @note
        min_coord, max_coord, and distinct_count must be integers, and must be set before __str__() may be called.
        max_coord will be used in place of calculate_chunk_length(), in case dim_high is '*'.
        In validate_output(), it is verified that dim_low <= min_coord <= max_coord <= dim_high, and distinct_count > 0
    """

    def __init__(self, dim_name, dim_low, dim_high, chunk_length, chunk_overlap):
        self.dim_name = dim_name;
        self.dim_low = dim_low;
        self.dim_high = dim_high;
        self.chunk_length = chunk_length;
        self.chunk_overlap = chunk_overlap;

        self.min_coord = 0
        self.max_coord = -1
        self.distinct_count = 0

        self._assigned_min_max_dc = False  # whether min_coord, max_coord, and distinct_count has been assigned.

        self.validate_after_input()

    def raise_exception(self, err_msg):
        """Raise an exception, prefixing the error message with the dimension name.

        The exception string is prefixed with the dimension name.
        @param err_msg  the error string.
        @exception AppError unconditionally.
        """
        str = 'Error! In dimension \'' + self.dim_name + '\': '
        str += err_msg
        raise scidblib.AppError(str)

    def set_min_max_dc(self, min_coord, max_coord, distinct_count):
        """Set min_coord, max_coord, and distinct_count.

        @param min_coord      the min coordinate.
        @param max_coord      the max coordinate.
        @param distinct_count the distinct count.
        @exception AppError if dim_low or dim_high are specified but do not satisfy
                dim_low <= min_coord <= max_coord <= dim_high,
                or if distinct_count <= 0.
        """
        try:
            self.min_coord = int(min_coord)
            self.max_coord = int(max_coord)
            self.distinct_count = int(distinct_count)
            self._assigned_min_max_dc = True
        except ValueError:
            assert False, 'set_min_max_dc() is called without integer input.'

        # dim_low <= min_coord (if dim_low is specified)
        if self.dim_low != '?':
            try:
                dim_low_int = int(self.dim_low)
            except ValueError:
                assert False, 'dim_low is neither \'?\' nor an integer; the error should have been caught earlier.'
            if dim_low_int > self.min_coord:
                self.raise_exception('The specified dim_low (=' + str(self.dim_low) +
                                 ') is larger than the actual min_coord (=' + str(self.min_coord) + ').')

        # min_coord <= max_coord
        if self.min_coord > self.max_coord:
            assert False, 'min_coord > max_coord.'

        # max_coord <= dim_high (if dim_high is specified and is not '*')
        if self.dim_high != '?' and self.dim_high != '*':
            try:
                dim_high_int = int(self.dim_high)
            except ValueError:
                assert False, 'dim_high is neither of \'?\', \'*\', or an integer;' + \
                                  'the error should have been caught earlier.'
            if (self.max_coord > dim_high_int):
                self.raise_exception('The specified dim_high (=' + str(self.dim_high) +
                                 ') is smaller than the actual max_coord (=' + str(self.max_coord) + ').')

        # distinct_count > 0
        if self.distinct_count <= 0:
            self.raise_exception('The calculated distinct_count is not positive.')

    def __str__(self):
        """Generate a string form of the dimension description.

        @return the generated string.
        @exception AssertionError if some fields in the dimension is not valid.
        """
        self.validate_before_output()
        return self.dim_name + '=' + str(self.dim_low) + ':' + str(self.dim_high) + ',' + str(self.chunk_length) + ',' + str(self.chunk_overlap);

    def validate_after_input(self):
        """Make sure the dimension specification is valid, right after input. '?' is allowed.

        @exception AppError if the dimension specification is not valid.
        """
        # name must exist
        if self.dim_name == '':
            assert False, 'The dimension name is empty; should have been caught earlier.'

        # all values must be integers or '?', except dim_high (which may be '*')
        if self.dim_low != '?':
            try:
                dim_low_int = int(self.dim_low)
            except ValueError:
                self.raise_exception('The specified dim_low is neither \'?\' nor an integer.')

        if self.chunk_length != '?':
            try:
                chunk_length_int = int(self.chunk_length)
            except ValueError:
                self.raise_exception('The specified chunk_length is neither \'?\' nor an integer.')
            if chunk_length_int <= 0:
                self.raise_exception('The specified chunk_length is not positive.');

        if self.chunk_overlap != '?':
            try:
                chunk_overlap_int = int(self.chunk_overlap)
            except ValueError:
                self.raise_exception('The specified chunk_overlap is neither \'?\' nor an integer.')
            if chunk_overlap_int < 0:
                self.raise_exception('The specified chunk_overlap is negative.')

        if self.dim_high != '?' and self.dim_high != '*':
           try:
                dim_high_int = int(self.dim_high)
           except ValueError:
                self.raise_exception('The specified dim_high is neither of \'?\', \'*\', or an integer.')
           if self.dim_low != '?':
                dim_low_int = int(self.dim_low)
                if dim_low_int > dim_high_int:
                    self.raise_exception('The specified dim_low is higher than dim_high.')


    def validate_before_output(self):
        """Make sure the dimension is ready for output.

        @exception AssertionError if not ready.
        """
        # name must exist
        if self.dim_name == '':
            assert False, 'The dimension name is empty; should have been caught earlier.'

        # all values must be integers, except dim_high (which may be '*')
        try:
            dim_low_int = int(self.dim_low)
        except ValueError:
            assert False, 'dim_low is not an integer.'

        try:
            chunk_length_int = int(self.chunk_length)
        except ValueError:
            assert False, 'chunk_length is not an integer.'

        try:
            chunk_overlap_int = int(self.chunk_overlap)
        except ValueError:
            assert False, 'chunk_overlap is not an integer.'

        # chunk_length should be positive;
        # chunk_overlap should be non-negative
        if chunk_length_int <= 0:
            assert False, 'chunk_length <= 0.'
        if chunk_overlap_int < 0:
            assert False, 'chunk_overlap < 0.'

        # dim_low <= min_coord <= max_coord
        if self._assigned_min_max_dc and dim_low_int > self.min_coord:
            assert False, 'dim_low > min_coord.'
        if self._assigned_min_max_dc and self.min_coord > self.max_coord:
            assert False, 'min_coord > max_coord.'

        # if dim_high is not '*', max_coord <= dim_high
        if self.dim_high != '*':
            try:
                dim_high_int = int(self.dim_high)
            except ValueError:
                assert False, 'dim_high is neither \'*\' nor an integer.'
            if self._assigned_min_max_dc and self.max_coord > dim_high_int:
                assert False, 'max_coord > dim_high.'

class Dimensions:
    """The specification of multiple dimensions.

    Details of public attributes:
      - list: a list of Dimension objects.
    """

    def __init__(self, s):
        """Given the dimension-specification part of a schema ('?' allowed), parse into Dimensions.

        @param s  string representation of the dimensions specification.
        All the five parts of a dimension specification must be specified.
        """
        self.list = []

        re_one = (
            r'\s*([^=\s]+)' +      # dim_name
            r'\s*=' +              # =
            r'\s*([^:\s]+)' +      # dim_low
            r'\s*:' +              # :
            r'\s*([^,\s]+)' +      # dim_high
            r'\s*,' +              # ,
            r'\s*([^,\s]+)' +      # chunk_length
            r'\s*,' +              # ,
            r'\s*([^,\s]+)' +      # chunk_overlap
            r'\s*'                 #
        )
        re_all_with_leading_comma =  r'^\s*,\s*' + re_one + r'(.*)$'
        re_all_without_leading_comma =  r'^\s*' + re_one + r'(.*)$'

        remains = s

        re_dim = re_all_without_leading_comma
        while remains:
            match_dim = re.match(re_dim, remains, re.M|re.I)
            if not match_dim:
                raise scidblib.AppError('Error! I cannot parse \'' + remains + '\'.\n' +
                                'It is expected to start with dim_name=dim_low:dim_high,chunk_length,chunk_overlap.')
            dim_name = match_dim.group(1)
            dim_low = match_dim.group(2)
            dim_high = match_dim.group(3)
            chunk_length = match_dim.group(4)
            chunk_overlap = match_dim.group(5)

            # Add to dimensions, after checking there is not a dimension with the same name.
            for dim in self.list:
                if dim_name == dim.dim_name:
                    raise scidblib.AppError('Error! There are multiple occurrences of the same dim_name (=' + dim_name + ').')
            self.list.append(Dimension(dim_name, dim_low, dim_high, chunk_length, chunk_overlap))

            remains = match_dim.group(6)
            re_dim = re_all_with_leading_comma

        if len(self.list)==0:
            raise scidblib.AppError('Error! Please specify at least one dimension.')

    def __str__(self):
        """Generate a string from Dimensions.

        @return the generated string
        @exception AppError if there is no dimension, or some dimension's __str__() encounters an error.
        """
        if len(self.list)==0:
            raise scidblib.AppError('System Error! There should be at least one dimension.')
        str_dims = []
        for dim in self.list:
            str_dims.append(dim.__str__())
        return ', '.join(str_dims)

class Attribute:
    """The specification of one SciDB-array attribute.

    Details of public attributes:
      - attr_name: the name of the attribute.
      - attr_type: the type of the attribute.
    """

    def __init__(self, attr_name, attr_type):
        self.attr_name = attr_name
        self.attr_type = attr_type

class Attributes:
    """The specification of multiple SciDB-array attributes.

    Details of public attributes:
      - list: a list of Attribute objects.
    """

    def __init__(self, s):
        """Given a string representation of some SciDB-array attributes, parse into Attributes.

        @param s  the string representation of the attributes.
        """
        self.list = []

        re_one = (
            r'\s*([^:\s]+)\b' +             # the attribute name
            r'\s*:' +                       # :
            r'\s*([^,\s]+)\b' +             # type
            r'(\s+(' +                      # begin of optional clauses
            '|'.join((
                      r'not\s+null\b',              #   - optional clause: not null
                      r'null\b',                    #   - optional clause: null
                      r'default\s+\\\'.*?\\\'',     #   - optional clause: default \'value_of_string_type\'
                      r'default\s+[^,\s]+\b',       #   - optional clause: default value_of_other_types
                      r'compression\s+[^,\s]+\b',   #   - optional clause: compression constant
                      r'reserve\s+[^,\s]+\b'         #   - optional clause: reserve constant
                      )) +
            r'))*' +                        # end of optional clause
            r'\s*'                          # trailing space
        )

        re_all_with_leading_comma =     r'^\s*,\s*' + re_one + r'(.*)$'
        re_all_without_leading_comma =  r'^\s*' + re_one + r'(.*)$'

        remains = s

        re_attr = re_all_without_leading_comma
        while remains:
            match_attr = re.match(re_attr, remains, re.M|re.I)
            if not match_attr:
                raise scidblib.AppError('Error! I cannot parse \'' + remains + '\'.\n' +
                                'It does not appear to contain valid attribute definition.')

            attr_name = match_attr.group(1)
            attr_type = match_attr.group(2)

            # Add to attrs, after checking there is not an attr with the same name.
            for attr in self.list:
                if attr_name == attr.attr_name:
                    raise scidblib.AppError('Error! There are multiple occurrences of the same attr_name (=' + attr_name + ').')
            self.list.append(Attribute(attr_name, attr_type))

            remains = match_attr.group(5)
            re_attr = re_all_with_leading_comma

class NameInLoadArray:
    """Information about one name (dimension or attribute) in the load array.

    Details of public attributes:
      - name           the name of the attribute or dimension.
      - is_dim         whether this is a dimension.
      - is_int64       True for dim and int64-typed attribute; False otherwise.
      - local_index    the index of this name in the list of attributes or dimensions (both 0-based).
      - min_coord      the min value of this name in load_array
      - max_coord      the max value of this name in load_array
      - distinct_count the distinct count of this name in load_array
    """

    def __init__(self, name, is_dim, is_int64, local_index):
        self.name = name
        self.is_dim = is_dim
        self.is_int64 = is_int64
        self.local_index = local_index
        self.min_coord = None       # place holder
        self.max_coord = None       # place holder
        self.distinct_count = None  # place holder

class NamesInLoadArray:
    """Information about all names (dimensions and attributes) in the load array.

    Details of public attributes:
      - list:  a list of NameInLoadArray objects.
      - dims:  a Dimensions object, storing info about the dimensions of the load_array.
      - attrs: an Attributes object, storing info about the attributes of the load_array.
    """

    def __init__(self, iquery_cmd, load_array):
        """Call iquery -aq "show(load_array)" to get the schema of the load array, and fill in data members.

        @param iquery_cmd  the iquery command.
        @param load_array  the name of the load array.
        @exception AppError if the show() command does not produce a valid schema,
                             e.g. if load_array is not a valid array name in the database.
        """
        self.list = []

        schema_str = scidb_afl.single_cell_afl(iquery_cmd, 'show(' + load_array + ')', 1)
        re_schema = (
            r'^.*' +             # array_name
            r'\<(.*)\>\s*' +     # <attributes>
            r'\[(.*)\]$'         # [dimensions]
        )

        match_schema = re.match(re_schema, schema_str, re.M|re.I)
        if not match_schema:
            raise scidblib.AppError('System Error! I failed to parse the schema of the load_array.')
        str_attrs = match_schema.group(1)
        str_dims = match_schema.group(2)

        # attributes
        self.attrs = Attributes(str_attrs)
        attrs = self.attrs.list
        for i, attr in enumerate(attrs):
            one_name = NameInLoadArray(attr.attr_name,
                                       is_dim = False,
                                       is_int64 = attr.attr_type=='int64',
                                       local_index = i)
            self.list.append(one_name)

        # dimensions
        self.dims = Dimensions(str_dims)
        dims = self.dims.list
        for i, dim in enumerate(dims):
            one_name = NameInLoadArray(dim.dim_name,
                                       is_dim = True,
                                       is_int64 = True,
                                       local_index = i)
            self.list.append(one_name)

    def find_index(self, name):
        """Given a name, find its index in self.list.

        @param name  the name to search for.
        @return the index of name in self.list.
        @exception AppError if the name does not exist.
        """
        for i, the_name in enumerate(self.list):
            if the_name.name == name:
                return i
        raise scidblib.AppError('System Error: the name \'' + name + '\' does not exist in NamesInLoadArray!')

    def gen_uniq_name(self):
        """Generate a temporary name, that is different from all the dimension or attribute names in load_array.

        @return the unique name.
        """
        name = 'tmp_name'
        uniq_suffix = 0
        found_uniq = False
        while not found_uniq:
            uniq_suffix += 1
            found_uniq = True
            uniq_name_candidate = name + str(uniq_suffix)
            for existing_name in self.list:
                if existing_name == uniq_name_candidate:
                    found_uniq = False
                    break;
        return name + str(uniq_suffix)

def calculate_chunk_length(args):
    """Calculate chunk length and other fields which were '?', and print out the schema.

    @param args  the result of argparse.ArgumentParser.parse_args().
    @return 0
    @exception AppError if anything goes wrong.
    """
    iquery_cmd = scidb_afl.get_iquery_cmd(args)
    load_array = args.load_array
    raw_dims_str = args.raw_dims

    calculated_dims = Dimensions(raw_dims_str)

    # Initialize the progress tracker
    progress_tracker = scidb_progress.ProgressTracker(sys.stdout,
                                      '',
                                      args.verbose,     # if_print_start
                                      args.verbose,     # if_print_end
                                      args.verbose      # if_print_skip
                                      )
    progress_tracker.register_step('min_max_dc', 'Get min_coord, max_coord, and ApproxDC for each dim from load_array.')
    progress_tracker.register_step('overall_dc', 'Get overall ApproxDC from load_array.')
    progress_tracker.register_step('calculate', 'Calculate and adjust dimension specification.')

    # S = dims where chunk_length is Specified;
    # N = dims where chunk_length is Not specified.
    S = []
    N = []
    for i, the_dim in enumerate(calculated_dims.list):
        if the_dim.chunk_length == '?':
            N.append(i)
        else:
            S.append(i)

    # Get the (dimension and attribute) names of the load_array.
    names_in_load_array = NamesInLoadArray(iquery_cmd, load_array)

    # for each i in [0..d), calculate min_coord[i], max_coord[i], and distinct_count[i]
    progress_tracker.start_step('min_max_dc')
    for the_dim in calculated_dims.list:
        index = names_in_load_array.find_index(the_dim.dim_name)
        the_name_in_load_array = names_in_load_array.list[index]

        if the_name_in_load_array.is_dim:
            tmp = names_in_load_array.gen_uniq_name()
            cmd = ('aggregate(apply(aggregate(' + load_array + ', count(*), ' + the_dim.dim_name +
                  '), ' + tmp + ', ' + the_dim.dim_name + '), min(' + tmp + '), max(' + tmp + '), count(*))'
                  )
        else:
            cmd = ('aggregate(' + load_array + ', min(' + the_dim.dim_name + '), max(' + the_dim.dim_name +
                   '), approxdc(' + the_dim.dim_name + '))'
                   )
        min_coord, max_coord, distinct_count = scidb_afl.single_cell_afl(iquery_cmd, cmd, 3)
        try:
            min_coord_int = int(min_coord)
            max_coord_int = int(max_coord)
            distinct_count_int = int(distinct_count)
            if args.verbose:
                print 'For ' + the_dim.dim_name + ', min_coord=' + str(min_coord_int) +\
                    ', max_coord=' + str(max_coord_int) +\
                    ', distinct_count=' + str(distinct_count_int)
        except ValueError:
            raise scidblib.AppError('Error: I cannot proceed because for ' + the_dim.dim_name + ' in array ' + load_array +
                            ', not all of min_coord (=' + min_coord + '), max_coord (=' + max_coord +
                            '), and distinct_count (=' + distinct_count + ') are integers.')
        the_dim.set_min_max_dc(min_coord_int, max_coord_int, distinct_count_int)
    progress_tracker.end_step('min_max_dc')

    # Fill dim_low, dim_high, and chunk_overlap (which was a '?' before).
    for the_dim in calculated_dims.list:
        if the_dim.dim_low == '?':
            the_dim.dim_low = the_dim.min_coord
        if the_dim.dim_high == '?':
            the_dim.dim_high = the_dim.max_coord
        if the_dim.chunk_overlap == '?':
            the_dim.chunk_overlap = 0

    # Generate string_concat_of_dim_values in the form of:
    # string(dim_name1) + '|' + string(dim_name2) + '|' + string(dim_name3)
    string_values = []
    for i, the_dim in enumerate(calculated_dims.list):
        string_values.append('string(' + the_dim.dim_name + ')')
    string_concat_of_dim_values = ' + \'|\' + '.join(string_values)

    # Calculate overall_distinct_count.
    tmp = names_in_load_array.gen_uniq_name()
    cmd = ('aggregate(apply(' + load_array + ', ' + tmp + ', ' + string_concat_of_dim_values + '), approxdc(' + tmp + '))'
           )
    progress_tracker.start_step('overall_dc')
    overall_distinct_count = scidb_afl.single_cell_afl(iquery_cmd, cmd, 1)
    overall_count = scidb_afl.single_cell_afl(iquery_cmd, 'aggregate(' + load_array + ', count(*))', 1)
    try:
        overall_distinct_count = int(overall_distinct_count)
        overall_count = int(overall_count)
        if overall_distinct_count > overall_count:
            overall_distinct_count = overall_count
    except ValueError:
        raise scidblib.AppError('Error: The query to get overall_distinct_count failed to return an integer.')
    if args.verbose:
        print 'overall_distinct_count=' + str(overall_distinct_count)
    progress_tracker.end_step('overall_dc')

    progress_tracker.start_step('calculate')

    # Shortcut: if |N| == 0, we are done.
    if len(N)==0:
        print calculated_dims.__str__()
        return 0

    # Set num_chunks_from_n.
    num_chunks_from_n = scidb_math.ceil_of_division(overall_distinct_count, args.desired_values_per_chunk)
    for i in S:
        the_dim = calculated_dims.list[i]
        chunk_count = scidb_math.ceil_of_division(the_dim.distinct_count, int(the_dim.chunk_length))
        num_chunks_from_n = scidb_math.ceil_of_division(num_chunks_from_n, chunk_count)
    if num_chunks_from_n <= 1:
        num_chunks_from_n = 1

    # For each dimension i in N, calculate chunk_count[i], then set chunk_length.
    for i in N:
        the_dim = calculated_dims.list[i]
        chunk_count = math.pow(num_chunks_from_n, 1.0/len(N))
        if not args.keep_shape:
            # calculate geomean
            product = 1.0
            for k in N:
                product *= calculated_dims.list[k].distinct_count
            geomean = math.pow(product, 1.0/len(N))
            chunk_count *= the_dim.distinct_count / geomean
        if chunk_count<1:
            chunk_count = 1.0
        the_dim.chunk_length = int(math.ceil(
                                           (the_dim.max_coord-the_dim.min_coord+1)/chunk_count
                                           ))
        if chunk_count>1:
            the_dim.chunk_length = scidb_math.snap_to_grid(
                                   the_dim.chunk_length, args.grid_threshold, use_binary=(not args.grid_base10))
    progress_tracker.end_step('calculate')

    # Print result.
    print calculated_dims.__str__()

    return 0

def main():
    """The main function gets command-line arguments and calls calculate_chunk_length().

    @return 0
    @exception AppError if something goes wrong.
    @note If print_traceback_upon_exception (defined at the top of the script) is True,
          stack trace will be printed. This is helpful during debugging.
    """
    parser = argparse.ArgumentParser(
                                     description='Chunk-length calculator (c) SciDB, Inc.\n' +
                                     '\n' +
                                     'The program calculates a dimension-specification string from a raw_dims string,\n' +
                                     'by replacing \'?\' with calculated values, for fields such as the chunk length.\n' +
                                     'The calculated string may be cut & pasted into the dimension-specification part\n' +
                                     'of a result-array schema, for the redimension() query.',
                                     epilog='examples:\n' +
                                     '  Suppose you have a SciDB array:\n' +
                                     '      arr_raw <i:int64,j:int64,v:double> [dummy=0:*,1000000,0],\n' +
                                     '  you want to redimension it into a matrix where i and j are dimensions, but you\n' +
                                     '  need help in choosing chunk lengths and/or low and high coordinates of the\n' +
                                     '  dimensions.\n' +
                                     '  You may call:\n' +
                                     '      calculate_chunk_length.py  arr_raw  \'i=?:?,?,?, j=?:?,?,?\'\n' +
                                     '  You are free to interleave \'?\' with values you desire. E.g. you may call:\n' +
                                     '      calculate_chunk_length.py  arr_raw  \'i=0:?,8192,0, j=?:?,?,10\'\n' +
                                     '\n' +
                                     'assumptions:\n' +
                                     '  - iquery is in your path.\n' +
                                     '  - The specified load_array exists in the database, and has data loaded.\n' +
                                     '  - Every specified dim_name in raw_dims must exist in load_array, either as a\n' +
                                     '    dimension, or as an attribute which is of type int64.\n' +
                                     '  - If you choose to specify a \'low\' (or \'high\') value for a dimension, it\n' +
                                     '    must be a lowerbound (or upperbound) of all actual values for that name in\n' +
                                     '    load_array.\n' +
                                     '\n' +
                                     'limitations:\n' +
                                     '  - The algorithm does not handle skew, e.g. when majority of the array is empty\n' +
                                     '    but there are a few small dense regions. In such cases, the script may\n' +
                                     '    produce overly large chunk lengths in that chunks covering the dense regions\n' +
                                     '    may use too much memory.\n' +
                                     '    The workaround is to reduce the desired_values_per_chunk argument.\n' +
                                     '  - The algorithm does not handle large-sized attributes, e.g. string attributes\n' +
                                     '    with thousands or even millions of bytes. In such cases, the script may\n' +
                                     '    produce overly large chunk lengths in that chunks of such attributes may\n' +
                                     '    use too much memory.\n' +
                                     '    The workaround is again to reduce the desired_values_per_chunk argument.',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('load_array',
                        help='The name of the source array to redimension from.')
    parser.add_argument('raw_dims',
                        help='''A string describing the dimension specification of the result array,
                        in the form of \'dim_name=low:high,chunk_length,chunk_overlap [, OTHER_DIMS]\'.
                        Some note:
                        (a) dim_name must be the name of either a dimension or an int64-typed attribute in load_array.
                        (b) The other four components are either an integer (the algorithm will respect that)
                          or \'?\' (the algorithm will calculate a value for it).
                        (c) The \'high\' component has an additional choice of \'*\', in which case the calculated
                          schema will also contains \'*\'.'''
                        )
    parser.add_argument('-c', '--host',
                        help='Host name to be passed to iquery.')
    parser.add_argument('-p', '--port',
                        help='Port number to be passed to iquery.')
    parser.add_argument('-d', '--desired_values_per_chunk', type=int, default=1024*1024,
                        help='''The number of desired non-empty values per chunk.
                        The default value is 1 Mebi (i.e. 2^20).
                        With the same desired values per chunk, a sparser result array will get at larger
                        chunk lengths.'''
                        )
    parser.add_argument('-k', '--keep_shape', action='store_true',
                        help='''If specified, the shape of a chunk will be similar to the shape of the array,
                        i.e. every dimension will be partitioned to a similar number of pieces.
                        The default is not keep_shape, i.e. a larger dimension will be partitioned to more pieces.'''
                        )
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='If specified, progress of the algorithm will be reported.')
    parser.add_argument('-t', '--grid_threshold', type=float, default=0.1,
                        help='''The default value is 0.1 (or 10%%).\n' +
                        The algorithm makes an effort to adjust each calculated chunk length to a
                        closeby \'gridline value\' (see the -g option).
                        A gridline value will be considered only if its relative difference from the
                        calculated chunk length is no more than this grid_threshold.
                        E.g. with the default value, if a calculated chunk length is 3847, the gridline value 4000
                        may be considered because its relative difference from 3847, i.e. (4000-3847)/3847=0.40, is
                        less than 0.1; but the gridline value 3000 may not be considered because its relative
                        difference from 3847 (=0.22) exceeds 0.1.
                        You may disable any adjustment by setting grid_threshold = 0.'''
                        )
    parser.add_argument('-g', '--grid_base10', action='store_true',
                        help='''If specified, use a multiple-of-power-of-10 as the gridline.
                        The default is not to specify, in which case a power-of-2 is used as the gridline.
                        In the power-of-2 case, there is only one candidate gridline value: the closest power of 2.
                        In the multiple-of-power-of-10 case, multiple candidates may be considered, with different
                        numbers of ending zeros. If multiple gridline values are within grid_threshold, the one with
                        the most number of ending zeros is chosen, breaking ties by favoring the one closer to the
                        calculated chunk length.
                        E.g. if a calculated chunk length is 3847, gridline values 10,000, 4000, 3800, and 3850 are
                        all considered. If grid_threshold=0.1, 4000 is chosen; if grid_threshold=0.01, 3850 is chosen.'''
                        )
    args = parser.parse_args()

    try:
        if args.desired_values_per_chunk <= 0:
            raise scidblib.AppError('Desired values per chunk must be positive.')
        exit_code = calculate_chunk_length(args)
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
