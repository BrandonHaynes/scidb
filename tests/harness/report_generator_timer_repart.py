#!/usr/bin/env python
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

import os
from itertools import chain
from math import sqrt

TIMER_NAMES=['repart_0', 'repart_1', 'repart_2']
TEST_NAMES=['value', 'tile']
COMPARE_TEST_NAME='value'
PRECISION=2

def align(v):
    return float(int(v * (10 ** PRECISION))) / 10 ** PRECISION

def parse_file(f):
    def parse_line(d, (n, l)):
        line = l.replace(' ', '').replace('ms', '').replace('\n', '').replace('\r', '')
        key_and_value = line.split(':')
        if len(key_and_value) != 2:
            raise ValueError("Can not parse string '%s' in %s line in file %s'" % 
                             (l, n, f.name))
        key, value = tuple(key_and_value)
        if key in TIMER_NAMES:
            if key in d:
                raise ValueError("Duplicate key %s in %s line in file '%s'" % 
                                 (key, n, f.name))
            d[key] = float(int(value))/1000
        return d
    return reduce(parse_line, enumerate(f.readlines()), {})
            
def process_file(d, n):
    if n in d:
        raise ValueError("File '%s' already processed" % n)
    df = parse_file(open(n))
    if set(df.keys()) != set(TIMER_NAMES):
        raise ValueError("Not completed file '%s'" % n)
    def average(l):
        ll = list(l)
        return sum(ll) / len(ll)
    dfv = df.values()
    name = n.replace('.timer', '')
    d[name] = {}
    d[name]['AVERAGE'] = align(average(dfv))
    d[name]['DEVIATION'] = align(sqrt(10 ** 4 * (average(v ** 2 for v in dfv) - (average(dfv) ** 2))) / average(dfv))
    return d

def process_dir(d, n):
    if n in d:
        raise ValueError("Directory '%s' already processed" % n)
    cwd = os.getcwd()
    try:
        os.chdir(n)
        root, dirnames, filenames = list(os.walk('.'))[0]
        d[n] = reduce(process_file, filenames, {})
        return d
    finally:
        os.chdir(cwd)

def print_result(d):
    result_names = list(chain(*[[name, 'deviance(%s)' % name] 
                                for name in TEST_NAMES]))
    compare_names = list('compare(%s)' % name 
                         for name in TEST_NAMES 
                         if name != COMPARE_TEST_NAME)
    column_names = ['case'] + result_names + compare_names
    print '\t'.join(column_names)
    case_names = list(set(chain(*[result.iterkeys() 
                                  for result in d.itervalues()])))
    case_names.sort()
    for case_name in case_names:
        result = []
        average = {}
        result.append(case_name)
        for test_name in TEST_NAMES:
            if case_name in d[test_name]:
                dc = d[test_name][case_name]
                average[test_name] = dc['AVERAGE']
                result.append('%sms' % dc['AVERAGE'])
                result.append('%s%%' % dc['DEVIATION'])
            else:
                result.append('N/A')
                result.append('N/A')
        for test_name in TEST_NAMES:
            if test_name == COMPARE_TEST_NAME:
                continue
            if case_name in d[test_name]:
                compare = align(average[COMPARE_TEST_NAME] / 
                                average[test_name])
                result.append('%sx' % compare)
            else:
                result.append('N/A')
        print '\t'.join(result)
        

if __name__ == "__main__":
    print_result(reduce(process_dir, TEST_NAMES, {}))
