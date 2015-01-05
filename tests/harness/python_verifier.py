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

import collections
import functools

ENABLED=True

__doc__= """
Python Verifier - check the number and type of expected arguments,
with usefull error messages.

Example of usage:

@verify(Str("first"),
        Str("second", a_cond=None),
        third=Int("third", a_none=True),
        collection=Iterable("collection", a_type=int, a_none=True))
def foo(first, second, third, collection=None):
    pass

This decorator verifies following:
  * first should be not-empty string (can not be None).
  * second should be string (can be empty, can not be None).
  * third should be int (can be None).
  * collection should be iterable and includes just int type items.
"""

class Var(object):
    """
    Variable type and condition verifier.
    """
    
    def __init__(self, a_name=None, a_desc=None, a_type=None, a_cond= None, a_none=False):
        """
        @param a_name: name of the variable/argument for verification.
        @type a_name: str

        @param a_desc: description of the variable/argument for doc string (optional).
        @type a_desc: None or str

        @param a_type: type of the variable/argument which expected.
            NOTE: in feature I plan to extend it by type list.
        @type a_type: type

        @param a_cond: condition which should be true (optional).
        @type a_cond: None or function with receive the argument and return bool.

        @param a_none: can be None
        @type a_none: bool
        """
        if not isinstance(a_name, str):
            raise TypeError("%s: expected a_name as str, "
                            "but received '%s'" % (self.__class__.__name__,
                                                 a_name))
        if not (a_desc is None or isinstance(a_desc, str)):
            raise TypeError("%s: expected a_desc as str or None, "
                            "but received '%s'" % (self.__class__.__name__,
                                                 a_desc))
        if not isinstance(a_type, type):
            raise TypeError("%s: expected a_type as type, "
                            "but received '%s'" % (self.__class__.__name__,
                                                a_type))
        if not isinstance(a_none, bool):
            raise TypeError("%s: expected a_none as bool, "
                            "but received '%s'" % (self.__class__.__name__,
                                                 a_none))
        if len(a_name) == 0:
            raise ValueError("%s: expected a_name as not-empty string, "
                             "but received '%s'" % (self.__class__.__name__,
                                                    a_name))
        if not (a_desc is None or len(a_desc)>0):
            raise ValueError("%s: expected a_desc as not-empty string or None, "
                             "but received '%s'" % (self.__class__.__name__,
                                                    a_desc))
        self._name=   a_name
        self._desc=   a_desc
        self._type=   a_type
        self._cond=   a_cond
        self._none=   a_none

    @property
    def name(self):
        """
        Name of the variable/argument for verification.
        """
        return self._name

    @property
    def desc(self):
        """
        Desription of the variable/argument for doc string (optional).
        """
        if self._desc is None:
            return '<description not provided>'
        else:
            return self._desc

    @property
    def type(self):
        """
        Type of the variable/argument which expected.
        """
        return self._type

    @property
    def cond(self):
        """
        Condition for check value, cond(value) should be True or value invalid (optional).
        """
        return self._cond

    @property
    def none(self):
        """
        Can be None
        """
        return self._none

    def wrap(self, value):
        """
        Wrap the value by additional checkers 
        (type verifier of items for iterable, for example).
        """
        return value

    def verify(self, caller, value):
        """
        Verify the variable.

        @param caller: the entity which acquire the verification.
        @type caller: any which have __name__ attribute.

        @param value: variable for the verification
        @type value: according to Var settings
        """
        if (not self.none) and (value is None):
            raise ValueError("%s: expected %s with type %s, "
                             "but received None" %
                             (caller.__name__, self.name, self.type.__name__))
        if self.none and not (value is None):
            value= self.wrap(value)
        if not ((self.none and (value is None)) or isinstance(value, self.type)):
            raise TypeError("%s: expected %s with type %s, "
                            "but received '%s' with type %s" %
                            (caller.__name__, self.name, self.type.__name__,
                             value, type(value).__name__))
        if not ((self.none and value is None) or (not self.cond) or self.cond(value)):
            if self.cond.__doc__ is None:
                cond_desc= '<description not provided>'
            else:
                cond_desc= self.cond.__doc__
            raise ValueError("%s: expected <%s> for %s, "
                             "but received '%s'" %
                             (caller.__name__, cond_desc, self.name,
                              value))

    @property
    def docstring(self):
        """
        Return the doc string for add to doc string of wrapped function/method.
        """
        return "@param %s: %s\n@type %s: %s" % (self.name, 
                                                self.desc, 
                                                self.name,
                                                self.type.__name__)


def not_empty_string(value):
    """not-empty string"""
    return len(value)>0

class Str(Var):
    def __init__(self, a_name, a_desc=None, 
                 a_cond=not_empty_string, a_none=False):
        """
        Verifier for the string. 
        By default expect the not-empty string.
        If you want accept the empty strings please set a_cond=None.
        """
        super(Str, self).__init__(a_name=a_name, 
                                     a_desc=a_desc,
                                     a_type=str,
                                     a_cond=a_cond,
                                     a_none=a_none)

class Bool(Var):
    def __init__(self, a_name, a_desc=None,
                 a_cond=None, a_none=False):
        """
        Verifier for the bool.
        """
        super(Bool, self).__init__(a_name=a_name,
                                   a_desc=a_desc,
                                   a_type=bool,
                                   a_cond=a_cond,
                                   a_none=a_none)

class Int(Var):
    def __init__(self, a_name, a_desc=None, a_cond=None, a_none=False):
        """
        Verifier for the int.
        """
        super(Int, self).__init__(a_name=a_name,
                                   a_desc=a_desc,
                                   a_type=int,
                                   a_cond=a_cond,
                                   a_none=a_none)

class Iterable(Var):
    def __init__(self, a_name, a_type=None, a_desc=None,
                 a_cond=None, a_none=False):
        """
        Verifier for the iterable.
        PLEASE NOTE: a_type here is type of the item in the iterable.
        PLEASE NOTE: current implemention convert the iterable to list.
        """
        if a_type:
            if not isinstance(a_type, type):
                raise TypeError("%s: expected a_type as type, "
                                "but receied %s" % (self.__class__.__name__,
                                                    a_type))
            def cond(value):
                cond.__doc__= "all items has %s type" % a_type.__name__
                return all(isinstance(item, a_type) for item in value)
        else:
            cond= None
        super(Iterable, self).__init__(a_name=a_name,
                                       a_desc=a_desc,
                                       a_type=collections.Iterable,
                                       a_cond=cond,
                                       a_none=a_none)

    def wrap(self, value):
        value= list(value)
        return value

class Proxy(Var):
    """
    Just proxy argument without checks.
    """
    def __init__(self):
        pass

    def wrap(self, value):
        return value

    def verify(self, caller, value):
        pass

def verify(*vargs, **vkwargs):
    """
    Return the decorator which verifies the *args and **kwargs

    @param a_caller: the entity which acquire the verification.
    @type a_caller: any which have __name__ attribute.

    @rtype: decorator
    """
    for var in vargs:
        if not isinstance(var, Var):
            raise TypeError("verify: expected Var or subclass of Var, "
                            "but received '%s' with type %s." % 
                            (var, type(var).__name__))
    for (key, var) in vkwargs.iteritems():
        if not isinstance(var, Var):
            raise TypeError("verify: expected Var %s or subclass of Var, "
                            "but received '%s' with type %s." % 
                            (key, var, type(var).__name__))
    if ENABLED:
        def decorator(f):
            """
            Decorator which verify the count and types of received arguments.
            """
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                if len(args) != len(vargs):
                    raise TypeError("%s: count of expected arguments (%s) and "
                                    "count of received arguments (%s) is different" 
                                    % (f.__name__, len(vargs), len(args)))
                for (expected, received) in zip(vargs, args):
                    expected.verify(f, received)
                kw_expected= set(vkwargs.iterkeys())
                kw_received= set(kwargs.iterkeys())
                kw_verify=   kw_expected.intersection(kw_received)
                for name in kw_verify:
                    vkwargs[name].verify(f, kwargs[name])
                return f(*args, **kwargs)
            return wrapper
    else:
        def decorator(f):
            return f
    return decorator

def verify_method(*vargs, **vkwargs):
    vargs= tuple([Proxy()] + list(vargs))
    return verify(*vargs, **vkwargs)

__all__=[ "Var", "Str", "Int", "Bool", "Iterable", "verify", "ENABLED" ]
