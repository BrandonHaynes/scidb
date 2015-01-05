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

from test_generator import *
from itertools import *
import unittest

class TestCheckArgumenType(unittest.TestCase):
    def check(self, type_value):
        return check_argument_type("TestCheckArgumentType", type_value)
    def test_correct(self):
        self.check(int)(5, "var int")
        self.check(float)(0.2, "var float")
        self.check(str)("test", "var str")
    def test_incorrect(self):
        with self.assertRaises(TypeError):
            check_argument_type()
        with self.assertRaises(TypeError):
            check_argument_type(None)
        caller_name= [ None, 5, int]
        type_value= [ None, "invalid", 5, "" ]
        for c, t in zip(caller_name, type_value):
            with self.assertRaises(TypeError):
                check_argument_type(c, t)
        for c in caller_name:
            with self.assertRaises(TypeError):
                check_argument_type(c, int)
        for t in type_value:
            with self.assertRaises(TypeError):
                check_argument_type("test", t)
        for t in type_value:
            with self.assertRaises(ValueError):
                check_argument_type("", t)


#def full_compare_test(test, one, two, three, incompatible=None, incomparable=None):
#    data= [one, two, three]
#    
#    comb= list(combinations(data, 2))
#    # test ==
#    for item in data:
#        test.assertEqual(item , item)
#        if incompatible:
#            with test.assertRaises(TypeError):
#                item == incompatible
#        if incomparable:
#            with test.assertRaises(ValueError):
#                item == incomparable
#    # test !=
#    for (a, b) in comb:
#        test.assertNotEqual(a, b)
#        if incompatible:
#            with test.assertRaises(TypeError):
#                item != incompatible
#        if incomparable:
#            with test.assertRaises(ValueError):
#                item != incomparable
#    # test <
#    for (a,b) in comb:
#        test.assertFalse(a < a)
#        test.assertTrue(a < b)
#        test.assertFalse(b < a)
#        if incompatible:
#            with test.assertRaises(TypeError):
#                a < incompatible
#        if incomparable:
#            with test.assertRaises(ValueError):
#                a < incomparable
#        if incompatible:
#            with test.assertRaises(TypeError):
#                incompatible < a
#        if incomparable:
#            with test.assertRaises(ValueError):
#                incomparable < a
#    # test <=
#    for (a,b) in comb:
#        test.assertTrue(a <= a)
#        test.assertTrue(a <= b)
#        test.assertFalse(b <= a)
#        if incompatible:
#            with test.assertRaises(TypeError):
#                a <= incompatible
#        if incomparable:
#            with test.assertRaises(ValueError):
#                a <= incomparable
#        if incompatible:
#            with test.assertRaises(TypeError):
#                incompatible <= a
#        if incomparable:
#            with test.assertRaises(ValueError):
#                incomparable <= a
#    # test >
#    for (a,b) in comb:
#        test.assertFalse(a > a)
#        test.assertFalse(a > b)
#        test.assertTrue(b > a)
#        if incompatible:
#            with test.assertRaises(TypeError):
#                a > incompatible
#        if incomparable:
#            with test.assertRaises(ValueError):
#                a > incomparable
#        if incompatible:
#            with test.assertRaises(TypeError):
#                incompatible > a
#        if incomparable:
#            with test.assertRaises(ValueError):
#                incomparable > a
#    # test >=
#    for (a,b) in comb:
#        test.assertTrue(a >= a)
#        test.assertFalse(a >= b)
#        test.assertTrue(b >= a)
#        if incompatible:
#            with test.assertRaises(TypeError):
#                a >= incompatible
#        if incomparable:
#            with test.assertRaises(ValueError):
#                a >= incomparable
#        if incompatible:
#            with test.assertRaises(TypeError):
#                incompatible >= a
#        if incomparable:
#            with test.assertRaises(ValueError):
#                incomparable >= a
#    # test cmp
#    for (a,b) in comb:
#        test.assertEqual(cmp(a, a), 0)
#        test.assertTrue(cmp(a, b) < 0)
#        test.assertTrue(cmp(b, a) > 0)
#        if incompatible:
#            with test.assertRaises(TypeError):
#                cmp(a, incompatible)
#        if incomparable:
#            with test.assertRaises(ValueError):
#                cmp(a, incomparable)
#        if incompatible:
#            with test.assertRaises(TypeError):
#                cmp(incompatible, a)
#        if incomparable:
#            with test.assertRaises(ValueError):
#                cmp(incomparable, a)
#
#class TestVector(unittest.TestCase):
#    def setUp(self):
#        self.cl= [0, 1, 2, -3]
#        self.cl2= self.cl[1:-1]
#        self.l= Vector([1, 1, 2, 0])
#        self.g= Vector([1, 1, 2, 4])
#        z= [ 0, 0,  0, 0]
#        l= [-0, 1, -2, 3]
#        self.zero= Vector(z)
#        self.one= Vector(l)
#        self.two= Vector(i*2 for i in self.one)
#        self.three= Vector(i+1 for i in self.two)
#        self.abs_one= Vector(abs(item) for item in l)
#
#    def vector(self):
#        return Vector(self.cl)
#
#    def test_create_valid_one(self):
#        self.assertIsInstance(self.vector(), Vector)
#        self.assertEqual(list(self.vector()), self.cl)
#
#    def test_create_valid_two(self):
#        v= Vector(self.cl2)
#        self.assertIsInstance(v, Vector)
#        self.assertEqual(list(v), self.cl2)
#
#    def test_create_iterable(self):
#        v= Vector(item for item in self.cl)
#        self.assertIsInstance(v, Vector)
#        self.assertEqual(list(v), self.cl)
#        self.assertEqual(list(self.vector()), self.cl)
#        with self.assertRaises(TypeError):
#            Vector(item for item in [])
#
#    def test_create_invalid_none(self):
#        with self.assertRaises(TypeError): 
#            Vector()
#
#    def test_create_invalid_empty(self):
#        with self.assertRaises(TypeError):
#            Vector([])
#
#    def test_create_invalid_string(self):
#        with self.assertRaises(TypeError):
#            Vector([0, "invalid"])
#
#    def test_compatible(self):
#        self.l.compatible(self.l)
#        self.l.compatible(self.g)
#        with self.assertRaises(TypeError):
#            self.l.compatible(Vector([0]))
#        with self.assertRaises(TypeError):
#            self.l.compatible(None)
#        with self.assertRaises(TypeError):
#            self.l.compatible("string")
#    def test_string(self):
#        self.assertEqual(str(self.one), '{0,1,-2,3}')
#        self.assertEqual(str(self.two), '{0,2,-4,6}')
#
#    def test_length(self):
#        self.assertEqual(len(self.vector()), len(self.cl))
#
#    def test_iter(self):
#        self.assertEqual(list(self.vector()), self.cl)
#
#    def test_getitem(self):
#        for index in xrange(0, len(self.vector())):
#            self.assertEqual(self.vector()[index], self.cl[index])
#            
#    def test_getitem_invalid(self):
#        with self.assertRaises(IndexError):
#            self.vector()[len(self.vector())]
#
#    def test_compare(self):
#        full_compare_test(self, self.one, self.two, self.three, incompatible=Vector([0, 1]))
#
#    def test_unary_minus(self):
#        self.assertEqual(-self.one, self.one - self.two)
#        self.assertEqual(-self.zero, self.zero)
#
#    def test_unary_plus(self):
#        self.assertEqual(+self.one, self.one)
#        self.assertEqual(+self.zero, self.zero)
#
#    def test_add(self):
#        self.assertEqual(self.one + self.one, self.two)
#        self.assertEqual(self.one + self.two, self.two + self.one)
#
#    def test_sub(self):
#        self.assertEqual(self.two - self.one, self.one)
#        self.assertEqual(self.one - self.two, self.two - self.one - self.two)
#
#    def test_min(self):
#        self.assertEqual(min(self.one, self.two), self.one)
#
#    def test_max(self):
#        self.assertEqual(max(self.one, self.two), self.two)
#
#    def test_hash(self):
#        self.assertEqual(hash(self.one), hash(self.one))
#        self.assertNotEqual(hash(self.one), hash(self.two))
#
#class TestBox(unittest.TestCase):
#    def setUp(self):
#        self.l= [2, -2, 2, -4]
#        self._double_min=   Vector([1, -2, 1, -4])
#        self._min=          Vector([2, -1, 2, -2])
#        self._max=          Vector([4, 1, 3, 0])
#        self._double_max=   Vector([8, 2, 4, 2])
#        self._incompatible= Vector([0, 1, 2])
#        self._diff= Vector([2, 0, 0, 0])
#
#    def box(self):
#        return Box(self._min, self._max)
#
#    def plain_box(self):
#        return Box(self._min+self._diff, self._max)
#
#    def incompatible_box(self):
#        return Box(self._incompatible, self._incompatible)
#
#    def test_create_invalid_types(self):
#        with self.assertRaises(TypeError):
#            Box(self._min, None)
#        with self.assertRaises(TypeError):
#            Box(None, self._max)
#        with self.assertRaises(TypeError):
#            Box("string", 5)
#
#    def test_create_incompatible(self):
#        with self.assertRaises(TypeError):
#            Box(self._min, self._incompatible)
#
#    def test_create(self):
#        self.assertIsInstance(self.box(), Box)
#        self.assertEqual(self.box().min(), self._min)
#        self.assertEqual(self.box().max(), self._max)
#
#    def test_create_plain_box(self):
#        self.assertIsInstance(self.plain_box(), Box)
#        self.assertEqual(self.plain_box().min(), self._min+self._diff)
#        self.assertEqual(self.plain_box().max(), self._max)
#
#    def test_create_invalid_minimum_greater_maximum(self):
#        with self.assertRaises(ValueError):
#            Box(self._max, self._min)
#
#    def test_create_total_plain_box(self):
#        b= Box(self._min, self._min)
#        self.assertIsInstance(b, Box)
#        self.assertEqual(b.min(), self._min)
#        self.assertEqual(b.max(), self._min)
#
#    def test_create_invalid_minimum_another_dimension_maximum(self):
#        with self.assertRaises(TypeError):
#            Box(self._min, Vector(self.l[:-1]))
#
#    def test_compatible(self):
#        self._min.compatible(self._min)
#        self._min.compatible(self._max)
#        with self.assertRaises(TypeError):
#            self._min.compatible(self.incompatible_box())
#        with self.assertRaises(TypeError):
#            self._min.compatible(None)
#        with self.assertRaises(TypeError):
#            self._min.compatible("string")
#
#    def test_not(self):
#        self.assertTrue(self.box())
#        self.assertFalse(self.plain_box())
#        self.assertFalse(not self.box())
#        self.assertTrue(not self.plain_box())
#
#    def test_min(self):
#        self.assertEqual(self.box().min(), self._min)
#
#    def test_max(self):
#        self.assertEqual(self.box().max(), self._max)
#
#    def test_len(self):
#        self.assertEqual(len(self.box()), 8)
#
#    def test_iter(self):
#        self.assertEqual(len(self.box()), len(list(self.box())))
#        s= set()
#        for item in self.box():
#            self.assertNotIn(item, s)
#            self.assertIn(item, self.box())
#            s.add(item)
#        self.assertEqual(';'.join(map(str, self.box())),
#                         '{2,-1,2,-2};{2,-1,2,-1};{2,0,2,-2};{2,0,2,-1};{3,-1,2,-2};{3,-1,2,-1};{3,0,2,-2};{3,0,2,-1}')
#
#    def test_contains(self):
#        self.assertTrue(self._min in Box(self._double_min, self._max))
#        self.assertFalse(self._max in self.box()) 
#        with self.assertRaises(TypeError):
#            self._incompatible in self.box()
#
#    def test_equal(self): 
#        self.assertEqual(self.box(), self.box())
#        with self.assertRaises(TypeError):
#            self.incompatible_box() == self.box()
#
#    def test_unequal(self):
#        self.assertNotEqual(self.box(), self.plain_box())
#        with self.assertRaises(TypeError):
#            self.incompatible_box() != self.box()
#
#    def test_hash(self):
#        self.assertEqual(hash(self.box()), hash(self.box()))
#        self.assertNotEqual(hash(self.box()), hash(self.plain_box()))
#
#    def not_intersected(self):
#        result= [
#            (Box(self._double_min,  self._min),
#             Box(self._min,        self._max)),
#            (Box(self._double_min,  self._min),
#             Box(self._max,        self._double_max)),
#            (Box(self._double_min,  self._max),
#             Box(self._max,        self._double_max)),
#            (Box(self._double_min,  self._double_max),
#             Box(self._double_max, self._double_max)),
#            (Box(self._min,         self._max),
#             Box(self._max,        self._max)),
#            (Box(self._min,         self._max),
#             Box(self._max,        self._double_max))]
#        return result + [(right, left) for (left, right) in result]
#
#    def superbox(self):
#        return [
#            (Box(self._min,        self._max),
#             Box(self._min,        self._max)),
#            (Box(self._double_min, self._max),
#             Box(self._min,        self._max)),
#            (Box(self._min,        self._double_max),
#             Box(self._min,        self._max)),
#            (Box(self._double_min, self._double_max),
#             Box(self._min,        self._max))]
#
#    def subbox(self):
#        return [ (subbox, superbox) for (superbox, subbox) in self.superbox() ]
#            
#
#    def intersected(self):
#        result= [ (subbox, superbox, subbox) for (subbox, superbox) in self.subbox()]
#        result.append((Box(self._double_min, self._max),
#                  Box(self._min,        self._double_max),
#                  Box(self._min,        self._max)))
#        return result + [(s, f, r) for (f, s, r) in result]
#    
#    def test_isdisjoint(self):
#        for (left, right) in self.not_intersected():
#            self.assertTrue(left.isdisjoint(right), 
#                            msg="%s.isdisjoint(%s)" % (str(left), str(right)))
#        for (left, right, _) in self.intersected():
#            self.assertFalse(left.isdisjoint(right), 
#                            msg="%s.isdisjoint(%s)" % (str(left), str(right)))
#        with self.assertRaises(TypeError):
#            self.incompatible_box().isdisjoint(self.box())
#
#    def test_intersection(self):
#        for (left, right) in self.not_intersected():
#            self.assertIsNone(left.intersection(right), 
#                              msg="%s.intersection(%s)" % (str(left), str(right)))
#        for (left, right, result) in self.intersected():
#            self.assertIsNotNone(left.intersection(right),
#                                 msg="%s.intersection(%s)" % (str(left), str(right)))
#            self.assertEqual(left.intersection(right), result,
#                             msg="%s.intersection(%s) == %s" % 
#                             (str(left), str(right), str(result)))
#        with self.assertRaises(TypeError):
#            self.incompatible_box().intersection(self.box())
#
#    def test_issubbox(self):
#        for (subbox, superbox) in self.subbox():
#            self.assertTrue(subbox.issubbox(superbox),
#                            msg="%s.issubbox(%s)" % (subbox, superbox))
#        with self.assertRaises(TypeError):
#            self.incompatible_box().issubbox(self.box())
#
#    def test_issuperbox(self):
#        for (subbox, superbox) in self.superbox():
#            self.assertTrue(subbox.issuperbox(superbox),
#                            msg="%s.issuperboxt(%s)" % (superbox, subbox))
#        with self.assertRaises(TypeError):
#            self.incompatible_box().issuperbox(self.box())
#
#class TestValue(unittest.TestCase):
#    def setUp(self):
#        self.a_list= [0, 1]
#        self.b_list= [2, 3]
#        self.c_list= [3, 4]
#        self.a= Value(self.a_list)
#        self.b= Value(self.b_list)
#        self.c= Value(self.c_list)
#        self.z= Value(None)
#    def test_create(self):
#        v= Value([0])
#        self.assertIsInstance(v, Value)
#        self.assertEqual(list(v), [0])
#    def test_create_iter(self):
#        v= Value(item for item in [0])
#        self.assertIsInstance(v, Value)
#        self.assertEqual(list(v), [0])
#    def test_create_none(self):
#        self.assertIsInstance(Value(None), Value)
#    def test_create_empty(self):
#        v= Value([])
#        self.assertIsInstance(v, Value)
#        self.assertEqual(list(v), [])
#    def test_create_iter(self):
#        v= Value(item for item in [0])
#        self.assertIsInstance(v, Value)
#        self.assertEqual(list(v), [0])
#    def test_compatible(self):
#        self.a.compatible(self.b)
#        with self.assertRaises(TypeError):
#            Value([0]).compatible(self.a)
#        with self.assertRaises(ValueError):
#            Value([0]).compatible(self.z)
#        with self.assertRaises(TypeError):
#            self.a.compatible(Vector[0])
#        with self.assertRaises(ValueError):
#            self.z.compatible(Value([0]))
#    def test_len(self):
#        self.assertEqual(len(self.a), 2)
#        self.assertEqual(len(self.b), len(self.a))
#        with self.assertRaises(ValueError):
#            len(self.z)
#
#    def test_iter(self):
#        self.assertEqual(list(self.a), self.a_list)
#        self.assertEqual(list(self.b), self.b_list)
#        self.assertNotEqual(list(self.a), self.b_list)
#        self.assertNotEqual(self.a_list, list(self.b))
#    def test_nonzero(self):
#        self.assertTrue(self.a)
#        self.assertTrue(self.b)
#        self.assertFalse(self.z)
#        self.assertIsNotNone(self.a)
#        self.assertIsNotNone(self.b)
#        self.assertIsNotNone(self.z)
#
#    def test_compare(self):
#        full_compare_test(self, self.a, self.b, self.c,
#                          incompatible=Value([0]),
#                          incomparable=Value(None))
#
#    def test_hash(self):
#        self.assertEqual(hash(self.a), hash(self.a))
#        self.assertNotEqual(hash(self.a), hash(self.b))
#
#    def test_string(self):
#        self.assertEqual(str(self.a), '(0,1)')
#        self.assertEqual(str(self.b), '(2,3)')
#        self.assertEqual(str(self.c), '(3,4)')
#
#class TestDimension(unittest.TestCase):
#    def test_invalid_name(self):
#        with self.assertRaises(ValueError):
#            Dimension()
#        with self.assertRaises(ValueError):
#            Dimension(name=0)
#    def test_invalid_minimum(self):
#        with self.assertRaises(ValueError):
#            Dimension(name="a")
#        with self.assertRaises(ValueError):
#            Dimension(name="a", minimum="invalid")
#    def test_invalid_maximum_and_count_null(self):
#        with self.assertRaises(ValueError):
#            Dimension(name="a", minimum=0)
#        with self.assertRaises(ValueError):
#            Dimension(name="a", minimum=0, maximum="invalid")
#        with self.assertRaises(ValueError):
#            Dimension(name="a", minimum=0, count="invalid")
#    def test_invalid_maximum(self):
#        with self.assertRaises(ValueError):
#            Dimension(name="a", minimum=0, maximum=-3)
#    def test_invalid_range_and_size(self):
#        with self.assertRaises(ValueError):
#            Dimension(name="a", minimum=0, maximum=4, size=3)
#        with self.assertRaises(ValueError):
#            Dimension(name="a", minimum=0, maximum=4, size="invalid")
#    def test_invalid_range_and_size_and_count(self):
#        with self.assertRaises(ValueError):
#            Dimension(name="a", minimum=0, maximum=4, size=2, count=3)
#    def test_create(self):
#        a= Dimension(name="a", minimum=0, maximum=4, size=2, count=2)
#        self.assertIsInstance(a, Dimension)
#        self.assertEqual(a.minimum, 0)
#        self.assertEqual(a.maximum, 4)
#        self.assertEqual(a.size, 2)
#        self.assertEqual(a.size, 2)
#        self.assertEqual(a.count, 2)
#    def test_create_without_count(self):
#        a= Dimension(name="a", minimum=0, maximum=4, size=2)
#        self.assertIsInstance(a, Dimension)
#        self.assertEqual(a.minimum, 0)
#        self.assertEqual(a.maximum, 4)
#        self.assertEqual(a.size, 2)
#        self.assertEqual(a.size, 2)
#        self.assertEqual(a.count, 2)
#    def test_create_without_maximum(self):
#        a= Dimension(name="a", minimum=0, size=2, count=2)
#        self.assertIsInstance(a, Dimension)
#        self.assertEqual(a.minimum, 0)
#        self.assertEqual(a.maximum, 4)
#        self.assertEqual(a.size, 2)
#        self.assertEqual(a.size, 2)
#        self.assertEqual(a.count, 2)
#    def test_create_method(self):
#        a= Dimension(name="a", minimum=0, size=2, count=2)
#        self.assertEqual(a.create(), "a=0:4,2,0")
#
#class TestDimensionList(unittest.TestCase):
#    def setUp(self):
#        self.a= Dimension(name="a", minimum=0, size=2, count=2)
#        self.b= Dimension(name="b", minimum=-1, size=2, count=2)
#    def test_create(self):
#        dl= DimensionList([self.a, self.b])
#        self.assertIsInstance(dl, DimensionList)
#        self.assertEqual(list(dl), [self.a, self.b])
#    def test_create_iter(self):
#        dl= DimensionList(item for item in [self.a, self.b])
#        self.assertIsInstance(dl, DimensionList)
#        self.assertEqual(list(dl), [self.a, self.b])
#    def test_create_invalid(self):
#        with self.assertRaises(ValueError):
#            DimensionList("invalid")
#        with self.assertRaises(ValueError):
#            DimensionList([])
#    def test_create_method(self):
#        dl= DimensionList([self.a, self.b])
#        self.assertEqual(dl.create(),"a=0:4,2,0,b=-1:3,2,0")
#
#class TestAttrubute(unittest.TestCase):
#    def test_create(self):
#        a= Attribute(a_name="name", a_type="int32")
#        self.assertIsInstance(a, Attribute)
#        self.assertEqual(a.name, "name")
#        self.assertEqual(a.type, "int32")
#    def test_invalid(self):
#        with self.assertRaises(ValueError):
#            Attribute()
#    def test_invalid_name(self):
#        with self.assertRaises(ValueError):
#            Attribute(a_name=0, a_type="int32")
#        with self.assertRaises(ValueError):
#            Attribute(a_name="", a_type="int32")
#    def test_invalid_type(self):
#        with self.assertRaises(ValueError):
#            Attribute(a_name="name", a_type=0)
#        with self.assertRaises(ValueError):
#            Attribute(a_name="name", a_type="")
#    def test_create_method(self):
#        self.assertEqual(Attribute(a_name="name", a_type="int32").create(), "name: int32")
#
#class TestAttributeList(unittest.TestCase):
#    def setUp(self):
#        self.a= Attribute(a_name="a", a_type="int32")
#        self.b= Attribute(a_name="b", a_type="int32")
#    def test_create(self):
#        al= AttributeList([self.a, self.b])
#        self.assertIsInstance(al, AttributeList)
#        self.assertEqual(list(al), [self.a, self.b])
#    def test_create_iter(self):
#        al= AttributeList(item for item in [self.a, self.b])
#        self.assertIsInstance(al, AttributeList)
#        self.assertEqual(list(al), [self.a, self.b])
#    def test_create_invalid(self):
#        with self.assertRaises(ValueError):
#            AttributeList("invalid")
#        with self.assertRaises(ValueError):
#            AttributeList([])
#    def test_create_method(self):
#        al= AttributeList([self.a, self.b])
#        self.assertEqual(al.create(),"a: int32,b: int32")
#
#class TestCell(unittest.TestCase):
#    def setUp(self):
#        self.vector= Vector([0, 1, 2])
#        self.value= Value(['a', 'b', 'c'])
#    def test_create(self):
#        c= Cell(self.vector, self.value)
#        self.assertIsInstance(c, Cell)
#        self.assertEqual(c.vector, self.vector)
#        self.assertEqual(c.value, self.value)
#    def test_invalid(self):
#        with self.assertRaises(TypeError):
#            Cell(None, None)
#        with self.assertRaises(TypeError):
#            Cell(self.vector, None)
#        with self.assertRaises(TypeError):
#            Cell(None, self.value)
#        with self.assertRaises(TypeError):
#            Cell(self.value, self.vector)
#        with self.assertRaises(TypeError):
#            Cell(self.value, Value['a', 'b'])
#

if __name__ == '__main__':
    unittest.main(verbosity=2)
