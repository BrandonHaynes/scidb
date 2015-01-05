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
#

import os
import sys
from itertools import product, imap, chain
from functools import wraps
from test_generator import *

# constant
SOURCE='source'
RESULT='result'

NAME='tile_repart'
SIZE='size'
COUNT='count'
TYPE='type'

# chunks sizes
CHUNK_SIZES='chunk_sizes'
SAME='same'
INCREASE='increase'
DECREASE='decrease'
CHUNK_SIZES_LIST=[INCREASE, DECREASE]

# overlap
OVERLAP='overlap'
OVERLAP_NONE='none'
OVERLAP_ADD='add'
OVERLAP_REMOVE='remove'
OVERLAP_INCREASE='increase'
OVERLAP_DECREASE='decrease'
OVERLAP_LIST=[OVERLAP_ADD, OVERLAP_REMOVE, OVERLAP_INCREASE, OVERLAP_DECREASE]

CASES_LIST = [(chunk_sizes_str, OVERLAP_NONE) for chunk_sizes_str in CHUNK_SIZES_LIST] + [(SAME, overlap_str) for overlap_str in OVERLAP_LIST]

def overlap_dict(range):
    multiplier = int(range / 20)
    assert(multiplier > 0)
    O_ZERO=    0
    O_VALUE=   multiplier
    O_LESSER=  multiplier
    O_GREATER= 2 * multiplier
    result = {
        OVERLAP_NONE:     { SOURCE: O_ZERO,    RESULT: O_ZERO },
        OVERLAP_ADD:      { SOURCE: O_ZERO,    RESULT: O_VALUE   },
        OVERLAP_REMOVE:   { SOURCE: O_VALUE,   RESULT: O_ZERO    },
        OVERLAP_INCREASE: { SOURCE: O_LESSER,  RESULT: O_GREATER },
        OVERLAP_DECREASE: { SOURCE: O_GREATER, RESULT: O_LESSER  }
        }
    for key in result.iterkeys():
        result[key]['NAME']= 'overlap_%s' % key
    return result

def chunk_sizes_dict(range, smaller, bigger):
    smaller_count = int(range / smaller)
    bigger_count = int(range / bigger)
    result = {
        SAME:     { SOURCE: { SIZE: bigger  },
                    RESULT: { SIZE: bigger  } },
        INCREASE: { SOURCE: { SIZE: smaller }, 
                    RESULT: { SIZE: bigger  } },
        DECREASE: { SOURCE: { SIZE: bigger  },
                    RESULT: { SIZE: smaller } }
        }
    for case in [SAME, INCREASE, DECREASE]:
        for array in [SOURCE, RESULT]:
            result[case][array][COUNT] = int(range / result[case][array][SIZE])
    for key in result.iterkeys():
        result[key]['NAME']= 'size_%s' %key
    return result

def settings_dict(range, smaller, bigger, overlap=None):
    cs = chunk_sizes_dict(range, smaller, bigger)
    if overlap is None:
        o = overlap_dict(smaller)
    else:
        o = overlap_dict(overlap)
    name = str(range)
    return { 'NAME': name, CHUNK_SIZES: cs, OVERLAP: o }

def substitute(settings, chunk_sizes_str, overlap_str):
    cs = settings[CHUNK_SIZES][chunk_sizes_str]
    o = settings[OVERLAP][overlap_str]
    name = '%s-%s-%s' % (settings['NAME'], cs['NAME'], o['NAME'])
    return { 'NAME': name, CHUNK_SIZES: cs, OVERLAP: o }


def create_dimension_list_2d(settings):
    cs = settings[CHUNK_SIZES]
    o = settings[OVERLAP]
    max = cs[SOURCE][SIZE] * cs[SOURCE][COUNT]
    source= []
    result= []
    for name in ['i', 'j']:
        source.append(Dimension(name=name, min=0, max=max,
                                size=cs[SOURCE][SIZE],
                                count=cs[SOURCE][COUNT],
                                overlap=o[SOURCE]))
        result.append(Dimension(name=name, min=0, max=max,
                                size=cs[RESULT][SIZE],
                                count=cs[RESULT][COUNT],
                                overlap=o[RESULT]))
    name = '2d-%s' % settings['NAME']
    return { 'NAME': name, SOURCE: source, RESULT: result }

def create_dimension_list_3d(third_settings, settings):
    cs = third_settings[CHUNK_SIZES]
    o= third_settings[OVERLAP]
    third_max = cs[SOURCE][SIZE] * cs[SOURCE][COUNT]
    source = [Dimension(name='z', min=0, max=third_max,
                        size=cs[SOURCE][SIZE],
                        count=cs[SOURCE][COUNT],
                        overlap=o[SOURCE])]
    result = [Dimension(name='z', min=0, max=third_max,
                        size=cs[RESULT][SIZE],
                        count=cs[RESULT][COUNT],
                        overlap=o[RESULT])]
    dl = create_dimension_list_2d(settings)
    for d in dl[SOURCE]:
        source.append(d)
    for d in dl[RESULT]:
        result.append(d)
    name = '3d-%s' % third_settings['NAME']
    return { 'NAME': name, SOURCE: source, RESULT: result }

def create_array(dimension_list):
    al = AttributeList([Attribute(a_name='a', a_type='int32')])
    source_dl = DimensionList(dimension_list[SOURCE])
    result_dl = DimensionList(dimension_list[RESULT])
    source = Array(name='source',attribute_list=al, dimension_list=source_dl)
    result = Array(name='result',attribute_list=al, dimension_list=result_dl)
    name = dimension_list['NAME']
    return { 'NAME': name, SOURCE: source, RESULT: result }

def create_array_2d(settings):
    return create_array(create_dimension_list_2d(settings))

def create_array_3d(third_settings, settings):
    return create_array(create_dimension_list_3d(third_settings, settings))

def generate_case_2d(d_settings):
    def result(chunk_sizes_str, overlap_str):
        return create_array_2d(substitute(d_settings, chunk_sizes_str, overlap_str))
    return result

def generate_case_3d(third_settings, settings):
    d_settings = substitute(settings, SAME, OVERLAP_NONE)
    def result(chunk_sizes_str, overlap_str):
        return create_array_3d(substitute(third_settings, chunk_sizes_str, overlap_str),
                               d_settings)
    return result

def iterate_arrays():
    sd1 = settings_dict(5000, 500, 1000)
    sd2 = settings_dict(10000, 5000, 10000)
    sd3 = settings_dict(20, 1, 2, overlap=20)
    g1 = generate_case_2d(sd1)
    g2 = generate_case_2d(sd2)
    g3 = generate_case_3d(sd3, sd1)
    return chain(generator(*case)
                 for generator in [g1, g2, g3]
                 for case in CASES_LIST)

def generate_test(s):
    for isSparse in [True, False]:
        if isSparse:
            name = 'sparse_%s'
        else:
            name = 'dense_%s'
        set_name((name % s['NAME']))
        source = s[SOURCE]
        result = s[RESULT]
        sdl = source.dimension_list
        build = '+'.join(["%s*%s" % (d.name, d.name) for d in sdl])
        if isSparse:
            sparse = ' or '.join([ "(%s %% %s = 0)" % (d.name, d.size) for d in sdl if d.size > 1])
            build = "build_sparse(source, %s, %s)" % (build, sparse)
        else:
            build = "build(source, %s)" % build
        setup = ['--setup', source.create(), result.create()]
        setup.append('--start-igdata')
        setup.append('--start-timer store')
        setup.append('store(%s, source)' % build)
        setup.append('--stop-timer store')
        setup.append('--stop-igdata')
        test = ['--test', '--start-igdata']
        test.append('store(repart(source, result), result)')
        for i in xrange(0, 3):
            test.append('--start-timer repart_%s' % i)
            test.append('store(repart(source, result), result)')
            test.append('--stop-timer repart_%s' % i)
        test.append('--stop-igdata')
        cleanup = [ '--cleanup', source.remove(), result.remove() ]
        write_test(setup + test + cleanup )

def clean_test(s):
    for isSparse in [True, False]:
        if isSparse:
            name = 'sparse_%s'
        else:
            name = 'dense_%s'
        set_name(name % s['NAME'])
        clean()

def unknown():
    sys.stderr.write('Error: unknown acion "%s"%s' % (sys.argv[1], os.linesep))
    exit(-1)

if __name__ == "__main__":
    set_suite(['perf', 'repart'])
    if len(sys.argv) != 2:
        sys.stderr.write('Error: specify action (generate or clean)%s' % os.linesep)
        exit(-1)
    action = { 'generate': generate_test, 'clean': clean_test }.get(sys.argv[1], unknown)
    map(action, iterate_arrays())
