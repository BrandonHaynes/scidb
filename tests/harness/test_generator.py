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
from operator import mul
from itertools import imap, product, izip
import python_verifier as pv

class Vector(object):
    'The n-dimensional vector'
    @pv.verify_method(pv.Iterable(a_name="vector", a_type=int))
    def __init__(self, vector):
        self._vector= vector

    def __hash__(self):
        return sum(map(hash, self))

    def __len__(self):
        'Total count of components'
        return len(self._vector)

    def __str__(self):
        'String representation of vector components'
        return '{%s}' % ','.join(map(str, self._vector))

    def __repr__(self):
        return "Vector%s" % str(self)

    #def dense(self):
    #    return ''

    #def sparse(self):
    #    return str(self)

    def __getitem__(self, index):
        "Get index's component"
        return self._vector[index]

    def __iter__(self):
        'Iterate over components'
        return iter(self._vector)

    def __neg__(self):
        return Vector(-item for item in self)

    def __pos__(self):
        return Vector(self)

    def compatible(self, right):
        pv.Var(a_name="right", a_type=Vector).verify(self.compatible, right)
        if len(self) != len(right):
            raise TypeError("Vector: compatible expects same "
                            "dimension count of vectors, "
                            "but self has %s and right has %s" %
                            (len(self), len(right)))

    @property
    def zero(self):
        return Vector(0 for value in self)

    def __cmp__(self, right):
        'Compare two vectors'
        self.compatible(right)
        for (left, right) in izip(self, right):
            result= cmp(left, right)
            if result != 0:
                return result
        return 0

    def __add__(self, right):
        self.compatible(right)
        return Vector((left + right) for (left,right) in zip(self, right))

    def __sub__(self, right):
        self.compatible(right)
        return Vector((left - right) for (left,right) in zip(self, right))

class Bound(object):
    @pv.verify_method(pv.Int("value"), pv.Int("overlap"), pv.Bool("has_overlap"))
    def __init__(self, value, overlap, has_overlap):
        self._value=       value
        self._overlap=     overlap
        self._has_overlap= has_overlap

    @property
    def value(self):
        return self._value

    @property
    def overlap(self):
        return self._overlap

    @property
    def has_overlap(self):
        return self._has_overlap

    def __str__(self):
        return ("Bound(value=%s,overlap=%s,has_overlap=%s)" % 
                tuple(map(str,[self.value, self.overlap, self.has_overlap])))

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return sum(map(hash, [self.value, self.overlap, self.has_overlap]))

    def __eq__(self, right):
        pv.Var(a_name="right", a_type=Bound).verify(self.__eq__, right)
        return all(left == right for left, right in [(self.value,       right.value),
                                                     (self.overlap,     right.overlap),
                                                     (self.has_overlap, right.has_overlap)])

    def __ne__(self, right):
        pv.Var(a_name="right", a_type=Bound).verify(self.__ne__, right)
        return not (self == right)

class Range(object):
    @pv.verify_method(pv.Var(a_name="minimum", a_type=Bound), 
                   pv.Var(a_name="maximum", a_type=Bound))
    def __init__(self, minimum, maximum):
        if minimum.overlap != maximum.overlap:
            raise ValueError("Range: overlap on minimum and maximum should be same, "
                             "but minimum has %s overlap and maximum has %s overlap" %
                             (minimum.overlap, maximum.overlap))
        if minimum.value > maximum.value:
            raise ValueError("Range: minimum should be lesser or equal than maximum, "
                             "but minimum is %s and maximum is %s" %
                             (minimum.value, maximum.value))
        self._minimum= minimum
        self._maximum= maximum

    @property
    def minimum(self):
        return self._minimum

    @property
    def maximum(self):
        return self._maximum

    @property
    def overlap(self):
        return self.minimum.overlap

    def __eq__(self, right):
        pv.Var(a_name="right", a_type=Range).verify(self.__eq__, right)
        return self.minimum == right.minimum and self.maximum == right.maximum

    def __ne__(self, right):
        pv.Var(a_name="right", a_type=Range).verify(self.__ne__, right)
        return not (self == right)

    def __hash__(self):
        return sum(map(hash, [self.minimum, self.maximum]))

    def __nonzero__(self):
        return self.minimum != self.maximum

    def __str__(self):
        return "Range(%s,%s)" % tuple(map(str, [self.minimum, self.maximum]))

    def __repr__(self):
        return str(self)

    def __contains__(self, right):
        pv.Var(a_name="right", a_type=Range).verify(self.__contains__, right)
        return self.minimum <= right and right < self.maximum

    def value_count(self):
        bound_list= [self.minimum, self.maximum]
        overlap_count= len(filter(None, map(Bound.has_overlap, bound_list)))
        return self.maximum.value - self.minimum.value + overlap_count * self.overlap

    def value_iter(self):
        minimum= self.minimum.value
        if self.minimum.has_overlap:
            minimum-= self.overlap
        maximum= self.maximum.value
        if self.maximum.has_overlap:
            maximum+= self.overlap
        return range(minimum, maximum)

class Box(object):
    @pv.verify_method(pv.Iterable(a_name="range_iter", a_type=Range))
    def __init__(self, range_iter):
        "every i'th range from range_iter - range from i'th dimension"
        self._range= range_iter

    def compatible(self, right):
        pv.Var(a_name="right", a_type=Box).verify(self.compatible, right)
        if len(self) != len(right):
            raise TypeError("Box: compatible expects same dimension count "
                            "of boxes, but self has %s and right has %s" %
                            (len(self), len(right)))

    def __nonzero__(self):
        return all(self)

    def __str__(self):
        return "Box(%s)" % ','.join(map(str,self))

    def __repr__(self):
        return str(self)

    def __contains__(self, right):
        'Vector inside the box'
        pv.Var(a_name="right", a_type=Box).verify(self.__contains__, right)
        if len(self) != len(right):
            raise TypeError("Box: contains expects same dimension count of "
                            "box and vector, but self has %s and right has %s" %
                            (len(self), len(right)))
        return all(_item in _range for _range, _item in izip(self, right))

    def __eq__(self, right):
        pv.Var(a_name="right", a_type=Box).verify(self.__eq__, right)
        self.compatible(right)
        return all(left == right for left, right in izip(self, right))

    def __ne__(self, right):
        pv.Var(a_name="right", a_type=Box).verify(self.__ne__, right)
        return not (self == right)

    def __hash__(self):
        return mul(map(hash, [self._min, self._max]))

    def __len__(self):
        return len(self._range)

    def __iter__(self):
        return iter(self._range)

    def cell_count(self):
        return 

    def vector_count(self):
        return mul(map(Range.value_count, self))

    def vector_iter(self):
        return imap(Vector, product(*imap(Range.value_iter, self)))

    @property
    def minimum(self):
        return Vector([current_range.minimum.value for current_range in self])
        

class Value(object):
    @pv.verify_method(pv.Iterable(a_name="value", a_type=int, a_none=True))
    def __init__(self, value):
        self._value= value

    def compatible(self, right):
        pv.Var(a_name="right", a_type= Value).verify(self.compatible, right)
        if bool(self) != bool(right):
            raise TypeError("Value: compatible expects both is None or both is "
                            "not None, but self is %s and right is %s" %
                            (self, right))
        if not self is None:
            if len(self) != len(right):
                raise TypeError("Value: compatible expects same dimension "
                                "count of values, but self has %s "
                                "and right has %s" % (len(self), len(right)))
        
    def __nonzero__(self):
        return not (self._value is None)

    def __len__(self):
        'Count of attributes'
        if self._value is None:
            raise ValueError("Value: is empty, len undefined")
        else:
            return len(self._value)

    def __iter__(self):
        'Iterate over attribute values (value should be not empty)'
        return iter(self._value)

    def __cmp__(self, right):
        'Compare two values'
        pv.Var(a_name="right", a_type= Value).verify(self.__cmp__, right)
        self.compatible(right)
        for (left, right) in izip(self, right):
            result= cmp(left, right)
            if result != 0:
                return result
        return 0

    def __hash__(self):
        return sum(map(hash, self))

    def __str__(self):
        if self._value is None:
            return '()'
        else:
            def to_string(value):
                if value is None:
                    return ''
                else:
                    return str(value)
            return '(%s)' % ','.join(map(to_string, self._value))

    def __repr__(self):
        return "Value%s" % str(self)

    #def dense(self):
    #    return str(self)

    #def sparse(self):
    #    return str(self)

class Dimension(object):
    def __init__(self, name=None, min=0,  max=None, 
                       size=None, count=None, overlap=0):
        if name is None or type(name) != str:
            raise TypeError("Dimension: incorrect name '%s'" % str(name))
        if  not len(name):
            raise ValueError("Dimension: incorrect name '%s'" % str(name))
        if min is None or type(min) != int:
            raise TypeError("Dimension: incorrect min %s" % str(min))
        if size is None or type(size) != int:
            raise TypeError("Dimension: incorrect size %s" % str(size))
        if size < 1:
            raise ValueError("Dimension: incorrect size %s" % str(size))
        if not (max is None or type(max) == int):
            raise TypeError("Dimension: incorrect max %s" % str(max))
        if not (max is None or min < max):
            raise ValueError("Dimension: max should be greater min")
        if not (count is None or type(count) == int):
            raise TypeError("Dimension: incorrect count %s" % str(count))
        if count < 1:
            raise ValueError("Dimension: incorrect count %s" % str(count))
        if max is None and count is None:
            raise ValueError("Dimension: please specify max or count")
        if max is None:
            assert(not count is None)
            max= min + size * count
        elif count is None:
            assert(not max is None)
            count= int((max - min) / size)
            if (max - min) % size != 0:
                count += 1
        else:
            bound_range = max - min
            count_range = size * count
            if bound_range != count_range and (bound_range >= count_range or 
                                               bound_range <= count_range - size):
                raise ValueError("Dimension: incompatible range, size and count, "
                                 "should (%s * %s)  == (%s - %s) or "
                                 "(%s * %s) < (%s - %s) < (%s * %s)" %
                                 (str(size), str(count), 
                                  str(max), str(min),
                                  str(size), str(count-1),
                                  str(max), str(min),
                                  str(size), str(count)))
        if overlap is None or type(overlap) != int:
            raise TypeError("Dimension: incorrect overlap %s" % str(overlap))
        if overlap < 0:
            raise ValueError("Dimension: incorrect overlap %s" % str(overlap))
        if overlap > size:
            raise ValueError("Dimension: overlap %s should lesser than size %s" %
                             (str(overlap), str(size)))
        self._name=     name
        self._min=      min
        self._max=      max
        self._size=     size
        self._count=    count
        self._overlap=  overlap

    @property
    def name(self):
        return self._name

    @property
    def minimum(self):
        return self._min

    @property
    def maximum(self):
        return self._max

    @property
    def size(self):
        return self._size

    @property
    def count(self):
        'Count of chunks'
        return self._count

    @property
    def overlap(self):
        return self._overlap

    def __len__(self):
        return self.count

    def __iter__(self):
        'Chunks ranges'
        def create_bound(index, has):
            return Bound(self.minimum + self.size * index, self.overlap, has)
        def create_range(index):
            minimum_has= index > 0
            maximum_hash= index + 1 < self.count
            minimum= create_bound(index, index > 0)
            maximum= create_bound(index+1, index + 1 < self.count)
            return Range(minimum, maximum)
        return imap(create_range, range(0, self.count))
 
    def __str__(self):
        return "Dimension(%s)" % self.create()

    def __repr__(self):
        return str(self)

    def create(self):
        l= [self.name, self.minimum, self.maximum-1, self.size, self.overlap]
        return '%s=%s:%s,%s,%s' % tuple(map(str, l))

class DimensionList(object):
    @pv.verify_method(pv.Iterable(a_name="dimension_iter", a_type=Dimension))
    def __init__(self, dimension_iter):
        self._list= list(dimension_iter)

    def __len__(self):
        return len(self._list)

    def __iter__(self):
        return iter(self._list)

    def box_count(self):
        return reduce(mul, [len(dimension) for dimension in self])

    def box_iter(self):
        # 1) from every i'th dimension receive the Range list. 
        #    As result, we have list of list of Range
        # 2) product Range lists (for get every combination of ranges)
        # 3) iterate over result from (2), receive arguments for Box, create Boxes        
        return [Box(range_list) for range_list in product(*map(iter,self))]

    def __str__(self):
        return "DimensionList<%s>" % self.create()

    def __repr__(self):
        return str(self)
        
    def create(self):
        return ','.join(map(Dimension.create, list(self)))

class Attribute(object):
    def __init__(self, a_name=None, a_type= None, a_default= None):
        if a_name is None or type(a_name) != str or not len(a_name):
            raise ValueError("Attribute: incorrect name %s" % str(a_name))
        if a_type is None or type(a_type) != str or not len(a_type):
            raise ValueError("Attribute: incorrect type %s" % str(a_type))
        self._name=    a_name
        self._type=    a_type
        # it is bad, should be changed in the feature.
        self._default= a_default

    @property
    def name(self):
        return self._name
    
    @property
    def type(self):
        return self._type
    
    def create(self):
        if self._default is None:
            default= ''
        else:
            default= ' DEFAULT %s' % self._default
        return '%s: %s%s' % (self._name, self._type, default)

class AttributeList(object):
    @pv.verify_method(pv.Iterable(a_name="attribute_iter", a_type=Attribute))
    def __init__(self, attribute_iter):
        self._list= list(attribute_iter)
    def __len__(self):
        return len(self._list)

    def __iter__(self):
        return iter(self._list)

    def create(self):
        return ','.join(map(Attribute.create, self._list))

class Cell(object):
    @pv.verify_method(pv.Var(a_name="vector", a_type=Vector),
                   pv.Var(a_name="value", a_type=Value))
    def __init__(self, vector, value):
        self._vector= vector
        self._value=  value

    @property
    def vector(self):
        return self._vector

    @property
    def value(self):
        return self._value

    #def dense(self):
    #    return "%s%s" % (self.vector.dense(), self.value.dense())

    #def sparse(self):
    #    return '%s%s' % (self.vector.sparse(), self.value.sparse())

class Chunk(object):
    @pv.verify_method(pv.Var(a_name="box", a_type=Box),
                   pv.Proxy())
    def __init__(self, box, filler):
        if filler is None:
            raise TypeError("Chunk: expected filler, but received %s" % str(box))
        self._box=    box
        self._filler= filler

    #def dense(self):
    #    @pv.verify(pv.Iterable(a_name="vector", a_type=int), 
    #               pv.Iterable(a_name="range_iter", a_type=Range))
    #    def build(vector, range_iter):
    #        range_list= list(range_iter)
    #        if len(range_list) == 0:
    #            v= Vector(vector)
    #            return Cell(v, Value(self._filler(v))).dense()
    #        else:
    #            current_range= range_list[0]
    #            range_list= range_list[1:]
    #            if len(range_list) > 0:
    #                comma= ',' % os.linesep
    #            else:
    #                comma= ','
    #            def convert(value):
    #                return build(vector + [value], range_list)
    #            value_iter= list(current_range.value_iter())
    #            return '[%s]' % comma.join(map(convert, value_iter))
    #    return build([], list(self._box))    

    #def sparse(self):
    #    def create_cell(vector):
    #        return Cell(vector, Value(self._filler(vector)))
    #    cell_list= [create_cell(vector) for vector in self._box.vector_iter()]
    #    cell_list= [cell.sparse() for cell in cell_list if cell.value]
    #    dimension_count= len(self._box)
    #    minimum= self._box.minimum
    #    prefix= '[' * dimension_count
    #    suffix= ']' * dimension_count
    #    return '%s%s%s%s' % (minimum, prefix, (',%s' % os.linesep).join(cell_list), suffix)

class Array(object):
    @pv.verify_method(name= pv.Str("name"),
                   attribute_list= pv.Iterable("attribute_list", a_type=Attribute),
                   dimension_list= pv.Iterable("dimension_list", a_type=Dimension),
                   filler= pv.Proxy())
    def __init__(self, name=None, attribute_list=None, dimension_list=None): #, filler=None):
        #if filler is None:
        #    raise TypeError("Array: expected filler, but received %s" % str(filler))
        self._name=      name
        self._attribute= attribute_list
        self._dimension= dimension_list
        #self._filler=    filler(self)

    @property
    def name(self):
        return self._name

    @property
    def attribute_list(self):
        return self._attribute

    @property
    def dimension_list(self):
        return self._dimension

    @property
    def filler(self):
        return self._filler

    def __len__(self):
        'Total count of chunks inside the array'
        return self._dimension.box_count()

    def __iter__(self):
        'Iterate over chunks inside the array'
        def create_chunk(box):
            return Chunk(box, self.filler)
        return imap(create_chunk, self._dimension.box_iter())

    #def dense(self):
    #    return self.cell_print(dense=True)

    #def sparse(self):
    #    return self.cell_print(dense=False)

    #@pv.verify_method(dense=pv.Bool("dense"))
    #def cell_print(self, dense=None):
    #    sep= ';%s' % os.linesep
    #    if dense:
    #        f= Chunk.dense
    #    else:
    #        f= Chunk.sparse
    #    return sep.join(map(f, list(self)))

    def create(self):
        return "create array %s <%s> [%s]" % (self.name, self.attribute_list.create(), self.dimension_list.create())

    def remove(self):
        return "remove(%s)" % self._name

#def sum_value_filler(array):
#    @pv.verify(pv.Var(a_name="vector", a_type=Vector))
#    def result(vector):
#        return [sum(vector) for _ in xrange(0, len(array.attribute_list))]
#    return result

#def empty_value_filler(_):
#    def result(_):
#        return None
#    return result
    

#def full_vector_filler(value_filler):
#    return value_filler

#def minimum_vector_filler(value_filler):
#    def result(array):
#        minimum= set(box.minimum for box in  array.dimension_list.box_iter()[:])
#        vf= value_filler(array)
#        ef= empty_value_filler(array)
#        def result(vector):
#            if vector in minimum:
#                return vf(vector)
#            else:
#                return ef(vector)
#        return result
#    return result

PREFIX= { 'TEST':      os.path.join('testcases', 't'),
          'RESULT':    os.path.join('testcases', 'r') }
SUFFIX= { 'TEST':      'test',
          'RESULT':    'expected' }
SUITE= None
NAME= None

def set_suite(suite):
    assert(type(suite) == list)
    assert(len(suite))
    path = os.path.join(PREFIX['TEST'], os.path.join(*suite))
    if not os.path.exists(path):
        os.makedirs(path)
    global SUITE
    SUITE = suite    

def set_name(name):
    assert(type(name) == str)
    assert(len(name))
    global NAME
    NAME = name

def get_path(kind):
    prefix = [ PREFIX[kind] ]
    suffix = ["%s.%s" % (NAME, SUFFIX[kind])]
    return os.path.join(*(prefix + SUITE + suffix))


def get_path_test():
    return get_path('TEST')

#def get_path_result():
#    return get_path('RESULT')

def write_file(name, data):
    f= open(name, 'w')
    f.write(data)
    f.close()
    print "Wrote: '%s'" % name

def write_test(lines):
    write_file(get_path_test(), os.linesep.join(lines))

def clean():
    try:
        os.remove(get_path_test())
        print "Removed: '%s'" % get_path_test()
    except OSError, e:
        print "Remove of %s completed with error '%s'" % (get_path_test(), str(e))

__all__ = [ "Vector", "Box", "Value", "Dimension", "DimensionList", "Attribute",
            "AttributeList", "Cell", "Chunk", "Array", 
            "set_suite", "set_name",
            "get_path_test", #"get_path_result",
            "write_test", "clean"]

