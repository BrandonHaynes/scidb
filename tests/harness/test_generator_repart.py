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
from itertools import product
from itertools import imap
from functools import wraps
from test_generator import *

# constant
SOURCE='source'
RESULT='result'

NAME='tile_repart'
SIZE='size'
COUNT='count'
TYPE='type'
MULTIPLIER=2

#dimension count
DIMENSION_LIST=[1, 2]

# maximum gap
MAX_GAP_ZERO='zero'
MAX_GAP_EXISTS='available'
MAX_GAP_LIST=[MAX_GAP_ZERO, MAX_GAP_EXISTS]


# chunks sizes
SAME='same'
ALIGNED_INCREASE='aligned_increase'
ALIGNED_DECREASE='aligned_decrease'
NOT_ALIGNED='not_aligned'
PARTIALLY_ALIGNED='partially_aligned'
CHUNK_SIZES_LIST=[SAME, ALIGNED_INCREASE, ALIGNED_DECREASE, NOT_ALIGNED, PARTIALLY_ALIGNED]

# overlap
OVERLAP_SAME='same'
OVERLAP_NONE='none'
OVERLAP_ADD='add'
OVERLAP_REMOVE='remove'
OVERLAP_INCREASE='increase'
OVERLAP_DECREASE='decrease'
OVERLAP_LIST=[OVERLAP_SAME, OVERLAP_NONE, OVERLAP_ADD, OVERLAP_REMOVE, OVERLAP_INCREASE, OVERLAP_DECREASE]

# density
#DENSE='dense'
#SPARSE='sparse'
#MIXED='mixed'
#DENSITY_LIST=[DENSE, SPARSE, MIXED]

# settings

MAX_GAP_DICT = { MAX_GAP_ZERO: 0, MAX_GAP_EXISTS: 1 }

O_ZERO=    0
O_VALUE=   1
O_LESSER=  1
O_GREATER= 2
OVERLAP_DICT= {
    OVERLAP_NONE:     { SOURCE: O_ZERO,    RESULT: O_ZERO    },
    OVERLAP_SAME:     { SOURCE: O_VALUE,   RESULT: O_VALUE   },
    OVERLAP_ADD:      { SOURCE: O_ZERO,    RESULT: O_VALUE   },
    OVERLAP_REMOVE:   { SOURCE: O_VALUE,   RESULT: O_ZERO    },
    OVERLAP_INCREASE: { SOURCE: O_LESSER,  RESULT: O_GREATER },
    OVERLAP_DECREASE: { SOURCE: O_GREATER, RESULT: O_LESSER  }
    }

CS_ZERO=    0
CS_VALUE=   2
CS_LESSER=  2
CS_GREATER= 3
CS_COPRIME_FIRST=  2
CS_COPRIME_SECOND= 3

O_ZERO            *= MULTIPLIER
O_VALUE           *= MULTIPLIER
O_LESSER          *= MULTIPLIER
O_GREATER         *= MULTIPLIER
CS_ZERO           *= MULTIPLIER
CS_VALUE          *= MULTIPLIER
CS_LESSER         *= MULTIPLIER
CS_GREATER        *= MULTIPLIER
CS_COPRIME_FIRST  *= MULTIPLIER
CS_COPRIME_SECOND *= MULTIPLIER

CHUNK_SIZES_DICT= {
    SAME:              { SOURCE: { SIZE: CS_VALUE,            COUNT: CS_VALUE            },
                         RESULT: { SIZE: CS_VALUE,            COUNT: CS_VALUE            } },
    ALIGNED_INCREASE:  { SOURCE: { SIZE: CS_LESSER,           COUNT: CS_GREATER          }, 
                         RESULT: { SIZE: CS_GREATER,          COUNT: CS_LESSER           } },
    ALIGNED_DECREASE:  { SOURCE: { SIZE: CS_GREATER,          COUNT: CS_LESSER           },
                         RESULT: { SIZE: CS_LESSER,           COUNT: CS_GREATER          } },
    NOT_ALIGNED:       { SOURCE: { SIZE: CS_COPRIME_FIRST,    COUNT: CS_COPRIME_SECOND   }, 
                         RESULT: { SIZE: CS_COPRIME_SECOND,   COUNT: CS_COPRIME_FIRST    } },
    PARTIALLY_ALIGNED: { SOURCE: { SIZE: CS_COPRIME_FIRST*2,  COUNT: CS_COPRIME_SECOND*2 },
                         RESULT: { SIZE: CS_COPRIME_SECOND*2, COUNT: CS_COPRIME_FIRST*2  } }
    }

def create_dimension_pair(name, chunk_sizes_str, overlap_str, max_gap_str):
    max_gap = MAX_GAP_DICT[max_gap_str]
    test_name = '-'.join([name, 
                          "size_" + chunk_sizes_str, 
                          "overlap_" + overlap_str,
                          "gap_" + max_gap_str])
    chunk_sizes = CHUNK_SIZES_DICT[chunk_sizes_str]
    overlap = OVERLAP_DICT[overlap_str]
    min = -MULTIPLIER
    max = chunk_sizes[SOURCE][SIZE] * chunk_sizes[SOURCE][COUNT] - max_gap
    max += min
    source = Dimension(name=name, min=min, max=max,
                       size=chunk_sizes[SOURCE][SIZE],
                       count=chunk_sizes[SOURCE][COUNT],
                       overlap=overlap[SOURCE])
    result = Dimension(name=name, min=min, max=max,
                       size=chunk_sizes[RESULT][SIZE],
                       count=chunk_sizes[RESULT][COUNT],
                       overlap=overlap[RESULT])
    return { 'TEST_NAME': test_name,
             SOURCE: source,
             RESULT: result }    

def iterate_dimension_pair(name):
    for chunk_sizes_str in CHUNK_SIZES_LIST:
        if chunk_sizes_str == SAME:
            continue
        for overlap_str in OVERLAP_LIST:
            for max_gap_str in MAX_GAP_LIST:
                yield create_dimension_pair(name, 
                                            chunk_sizes_str,
                                            overlap_str,
                                            max_gap_str)

def create_array_pair(dimension_pair_list):
    test_name = '--'.join([d['TEST_NAME'] for d in dimension_pair_list])
    al = AttributeList([Attribute(a_name='a', a_type='int32', a_default=100)])
    source_dl = DimensionList([d[SOURCE] for d in dimension_pair_list])
    result_dl = DimensionList([d[RESULT] for d in dimension_pair_list])
    source = Array(name='source',attribute_list=al, dimension_list=source_dl)
    result = Array(name='result',attribute_list=al, dimension_list=result_dl)
    return { 'TEST_NAME': test_name, SOURCE: source, RESULT: result }

def iterate_arrays():
    y = create_dimension_pair('y', SAME, OVERLAP_SAME, MAX_GAP_ZERO)
    yield create_array_pair([y])
    for x in iterate_dimension_pair('x'):
        yield create_array_pair([x])
        yield create_array_pair([x, y])    
        yield create_array_pair([y, x])    
        

def generate(d):
    set_name(d['TEST_NAME'])
    source = d[SOURCE]
    result = d[RESULT]
    build = None
    for d in source.dimension_list:
        if build is None:
            build = d.name
        else:
            build = "(%s)*%s+%s" % (build, d.maximum - d.minimum, d.name)
    build = "build(source, %s)" % build
    setup = ['--setup',
             source.create(), 
             result.create(), 
             'store(%s, source)' % build ]
    test = ['--test', 
            'scan(source)',
            'repart(source, result)',
            'store(repart(source, result), result)',
            'scan(result)']
    cleanup = [ '--cleanup',
                source.remove(),
                result.remove() ]
    write_test(setup + test + cleanup)

def clean(d):
    set_name(d['TEST_NAME'])
    clean()

def unknown():
    sys.stderr.write('Error: unknown acion "%s"%s' % (sys.argv[1], os.linesep))
    exit(-1)

if __name__ == "__main__":
    set_suite(['checkin', 'other', 'repart'])
    if len(sys.argv) != 2:
        sys.stderr.write('Error: specify action (generate or clean)%s' % os.linesep)
        exit(-1)
    action = { 'generate': generate, 'clean': clean }.get(sys.argv[1], unknown)
    map(action, iterate_arrays())
