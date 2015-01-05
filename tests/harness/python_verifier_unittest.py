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

import unittest
from python_verifier import *

class TestVar(unittest.TestCase):
    def test_init(self):
        with self.assertRaises(TypeError):
            Var()
        with self.assertRaises(TypeError):
            Var(a_name=5)
        with self.assertRaises(TypeError):
            Var(a_name="")
        with self.assertRaises(TypeError):
            Var(a_name="name")
        with self.assertRaises(TypeError):
            Var(a_name="name", a_type="int")
        with self.assertRaises(TypeError):
            Var(a_name="name", a_type=5)
        with self.assertRaises(TypeError):
            Var(a_name="name", a_type=int, a_desc=5)
        with self.assertRaises(ValueError):
            Var(a_name="name", a_type=int, a_desc="")
        Var(a_name="name", a_type=int, a_desc="desc")
        Var(a_name="name", a_type=int, a_desc="desc", a_cond=5)
        Var(a_name="name", a_type=int, a_desc="desc", a_cond=lambda(v): v> 0)
        with self.assertRaises(TypeError):
            Var(a_name="name", a_type=int, a_desc="desc",
                a_cond=lambda(v): v> 0, a_none=None)
        with self.assertRaises(TypeError):
            Var(a_name="name", a_type=int, a_desc="desc", 
                a_cond=lambda(v): v> 0, a_none=5)
        Var(a_name="name", a_type=int, a_desc="desc", a_cond=lambda(v): v> 0, a_none=True)

    def test_value(self):
        def cond(v):
            "positive number"
            return v>0
        v= Var(a_name="c", a_desc="desc", a_type=int, a_cond=cond)
        with self.assertRaises(ValueError):
            v.verify(self.test_value, None)
        with self.assertRaises(ValueError):
            v.verify(self.test_value, 0)
        v.verify(self.test_value, 5)
        v= Var(a_name="c", a_desc="desc", a_type=int, 
               a_cond=cond, a_none=True)
        v.verify(self.test_value, None)
        with self.assertRaises(ValueError):
            v.verify(self.test_value, 0)
        v.verify(self.test_value, 5)
        v= Var(a_name="c", a_desc="desc", a_type=int, a_cond=5)
        with self.assertRaises(ValueError):
            v.verify(self.test_value, None)
        with self.assertRaises(TypeError):
            v.verify(self.test_value, 0)
        with self.assertRaises(TypeError):
            v.verify(self.test_value, 5)

class TestStr(unittest.TestCase):
    def test_init(self):
        Str("name")
        with self.assertRaises(TypeError):
            Str(5)

    def test_value(self):
        s= Str("name")
        s.verify(self.test_value, "value")
        with self.assertRaises(ValueError):
            s.verify(self.test_value, "")
        with self.assertRaises(ValueError):
            s.verify(self.test_value, None)
        with self.assertRaises(TypeError):
            s.verify(self.test_value, 5)

class TestBool(unittest.TestCase):
    def test_init(self):
        Bool("name")
        with self.assertRaises(TypeError):
            Bool(5)

    def test_value(self):
        s= Bool("name")
        s.verify(self.test_value, True)
        with self.assertRaises(TypeError):
            s.verify(self.test_value, "")
        with self.assertRaises(ValueError):
            s.verify(self.test_value, None)
        with self.assertRaises(TypeError):
            s.verify(self.test_value, 5)

class TestInt(unittest.TestCase):
    def test_init(self):
        Int("name")
        with self.assertRaises(TypeError):
            Int(5)

    def test_value(self):
        s= Int("name")
        s.verify(self.test_value, 5)
        with self.assertRaises(TypeError):
            s.verify(self.test_value, "")
        with self.assertRaises(ValueError):
            s.verify(self.test_value, None)
        with self.assertRaises(TypeError):
            s.verify(self.test_value, int)

class TestIterable(unittest.TestCase):
    def test_init(self):
        Iterable("name")
        with self.assertRaises(TypeError):
            Iterable(5)

    def test_value(self):
        i= Iterable("collection")
        i.verify(self.test_value, [0, 1, 2, "test"])
        i.verify(self.test_value, iter(item for item in [0, 1, 2, "type"]))
        i= Iterable("collection", a_type=int)
        with self.assertRaises(ValueError):
            i.verify(self.test_value, [0, 1, 2, "test"])
        with self.assertRaises(ValueError):
            i.verify(self.test_value, iter(item for item in [0, 1, 2, "type"]))
        i.verify(self.test_value, [0, 1, 2])
        i.verify(self.test_value, iter(item for item in [0, 1, 2]))

class TestVerify(unittest.TestCase):
    def test_init(self):
        v= verify(Str("the_string"), Int("the_int_can_be_None", a_none=True),
                  the_bool=Bool("the_bool"),
                  the_coll=Iterable("the_coll", a_type=int))
        class Fake(Exception):
            pass
        @v
        def foo(a,b, the_bool=None, the_coll=None):
            raise Fake()
        with self.assertRaises(TypeError):
            foo("string")
        with self.assertRaises(TypeError):
            foo(5)
        with self.assertRaises(TypeError):
            foo("string", 5, another=None)
        with self.assertRaises(Fake):
            foo("string", 5)
        with self.assertRaises(TypeError):
            foo("string", 5, the_bool=5)
        with self.assertRaises(Fake):
            foo("string", 5, the_bool=True, the_coll=[0,1,2])
        with self.assertRaises(ValueError):
            foo("string", 5, the_bool=True, the_coll=[0,1,"test"])
        with self.assertRaises(Fake):
            foo("string", 5, the_bool=True, the_coll=iter(item for item in [0,1]))

if __name__ == '__main__':
    unittest.main(verbosity=2)
    
