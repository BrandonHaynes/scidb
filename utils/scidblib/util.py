#!/usr/bin/python

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

# BEGIN_COPYRIGHT_EXCEPTIONS
#
# All code in this file is covered by the AGPL3 copyright detailed
# above, with the following explicit exceptions.  See doc strings for
# attribution and/or copyright information.
#
#   - superTuple
# 
# END_COPYRIGHT_EXCEPTIONS

from operator import itemgetter

def superTuple(typename, *attribute_names):
    """Create and return a subclass of 'tuple', with named attributes.

    @see "Python Cookbook", Martelli et. al., O'Reilly, 2nd ed.,
    recipe 6.7 "Implementing Tuples with Named Items".
    """
    # Make the subclass with appropriate __new__ and __repr__ methods.
    nargs = len(attribute_names)
    class supertup(tuple):
        __slots__ = () # save memory, we don't need per-instance dict
        def __new__(cls, *args):
            if len(args) != nargs:
                raise TypeError("%s takes exactly %d arguments (%d given)" % (
                    typename, nargs, len(args)))
            return tuple.__new__(cls, args)
        def __repr__(self):
            return '%s(%s)' % (typename, ', '.join(map(repr, self)))
    # Set up attribute names for gettable indices.
    for i, attr_name in enumerate(attribute_names):
        setattr(supertup, attr_name, property(itemgetter(i)))
    supertup.__name__ = typename
    return supertup
