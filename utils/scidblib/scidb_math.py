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
import math
import re
import scidblib

def comma_separated_number(value, sep=',', dot='.'):
    """Return the string representation of a value, where every three digits are separated by a comma.

    @param value  the input value.
    @param sep    comma
    @param dot    symbol in the input value
    @return a string representation of the value where every three digits are separated by a comma.
    @exception ValueError if the input parameter is not of numeric type.
    @note If you have Python 2.7, the way to do is
          return "{:,}".format(value)
    """
    if not isinstance(value, (int, long, float)):
        raise ValueError('comma_separated_number can only be used on numeric types.')
    num, _, frac = str(value).partition(dot)
    num = re.sub(r'(\d{3})(?=\d)', r'\1'+sep, num[::-1])[::-1]
    if frac:
        num += dot + frac
    return num

def fraction_if_less_than_one(value):
    """Return the string form of a value (if >= 1) or 1 over its reciprocal (if < 1).

    @param value  a positive number.
    @return string form of the value (if >= 1), or 1 over its reciprocal (if < 1).
    @exception ValueError if value is not numeric.
    @exception AssertionError if value is not positive.
    @note commas will be added to separate every three digits.
    """
    assert value>0

    ret = ''
    if value < 1:
        value = 1/value
        ret += '1/'
    if int(value)==value:
        value = int(value)
    ret += comma_separated_number(value)
    return ret

def ceil_of_division(n, d):
    """Compute the ceil() of the division of two integers as an integer.

    @param n numerator.
    @param d denominator.
    @return the ceil() of n/d as an integer.
    @exception ValueError if d is 0.
    """
    return int(math.ceil(n*1.0/d))

def round_up(n, k):
    """Return the smallest integer >= n, where the lowest k digits are 0.

    @param n: a positive integer to round up.
    @param k: a positive integer indicating how many digits should be 0.
    @return the round-up number.
    @exception AssertError if either n or k is not a positive integer.
    @example
      - round_up(3140, 1) = 3140:  no change because the lowest digit is already 0.
      - round_up(3140, 2) = 3200:  the lowest two digits are 0.
      - round_up(3140, 3) = 4000:  the lowest three digits are 0.
      - round_up(3140, k>=4) = 1 followed by k zeros.
    """
    assert isinstance(n, (int, long)) and isinstance(k, (int, long)) and n>0 and k>0

    # how many digits does n have?
    num_digits = len(str(abs(n)))

    # 1 followed by k zeros.
    k_zeros = int(math.pow(10, k))

    if k >= num_digits: # if k >= num_digits, return 1 followed by k zeros.
        return k_zeros
    elif n % k_zeros == 0: # if the last k digits are already zeros, just return n.
        return n
    else:
        return (n//k_zeros+1) * k_zeros # increase the (k+1)'th digit by 1.

def round_down(n, k):
    """Return the largest integer <= n, where the lowest k digits are 0.

    @param n: a positive integer to round up.
    @param k: a positive integer indicating how many digits should be 0.
    @return the round-down number.
    @exception AssertError if either n or k is not a positive integer.
    @example
      - round_down(3140, 1) = 3140:  no change because the lowest digit is already 0.
      - round_down(3140, 2) = 3100:  the lowest two digits are 0.
      - round_down(3140, 3) = 3000:  the lowest three digits are 0.
      - round_down(3140, 4) = 0: the lowest four digits are 0.
    """
    assert isinstance(n, (int, long)) and isinstance(k, (int, long)) and n>0 and k>0
    k_zeros = int(math.pow(10, k))
    return n//k_zeros * k_zeros

def snap_to_grid(input_value, threshold, use_binary=True):
    """Get the 'nearest gridline value' if close, or the input_value.

    A gridline value is close, if its relative difference from the input value is within a given threshold.

    The 'nearest gridline value' is defined as follows.
      - If use_binary==False:
        use the multiple-of-power-of-10 with the most number of ending zeros,
        among those close to the input value;
        breaking ties by choosing the closest the one which is the closest to the input value.
      - If use_binary==True:
        choose the closest power of 2.
    @example
      - snap_to_grid(3161, 0.01, use_binary=False) = 3160
        The difference, 3161-3160=1, is within 1% of 3161.
        If we omit more digits, either using 3200 or 3100, the difference will exceed 1%.
      - snap_to_grid(3161, 0.1, use_binary=False)  = 3000
        The difference, 3161-3000=161, is within 10% of 3161.
        Here 3200 is closer to 3161 than 3000 is; however the function prefers 3000 because it has more ending zeros.
      - snap_to_grid(1021, 0.01, use_binary=True)  = 1024
        The difference, 1024-1021=3, is within 10% of 1021.
        Here the gridline is the nearest power of 2.
    @param input_value: a positive integer.
    @param threshold: the upperbound on the relative difference.
    @param use_binary: True means to use a power-of-2; False means to use a multiple-of-power-of-10.
                       Default is True.
    @return the nearest gridline value, if close.
    @exception AssertError if the input is not a positive integer.
    """
    assert isinstance(input_value, (int, long)) and input_value>0, \
        'Only positive integers may be used to call snap_to_grid.'

    if use_binary:
        # get p, s.t. 2^p <= input_value < 2^(p+1)
        p = 0
        tmp = input_value
        while tmp > 1:
            p += 1
            tmp >>= 1

        # set grid = 2^p or 2^(p+1), whichever is closer to input_value, and set diff = the non-negative difference
        grid = int(math.pow(2, p))
        assert grid <= input_value and input_value < grid*2
        diff = input_value - grid
        if grid*2-input_value < diff:  # the higher grid is closer
            grid = grid*2
            diff = grid - input_value

        # if the grid value is close, return it; otherwise, return the original value
        if diff <= input_value*threshold:
            return grid
        return input_value
    else:
        num_digits = len(str(abs(input_value)))
        num_digits_left = num_digits
        while num_digits_left > 0:
            grid = round_up(input_value, num_digits_left)
            diff = grid - input_value
            grid_down = round_down(input_value, num_digits_left)
            diff_down = input_value - grid_down
            if grid_down > 0 and diff_down < diff:
                grid = grid_down
                diff = diff_down
            if diff <= input_value*threshold:
                return grid
            num_digits_left -= 1
        return input_value

def geomean(values):
    """Compute geomean of a list of values.

    @param values: a list of values.
    @return the geomean of the values.
    @exception ValueError if the list is empty, or if some element is not a value.
    """
    if len(values) == 0:
        raise ValueError('I cannot compute geomean over an empty list.')
    product = reduce(
      lambda prod,val: prod*val, # accumulation
      values, 
      1.0 # initial product value
    )
    return math.pow( product, 1.0/len(values) )

