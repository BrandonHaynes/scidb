#!/usr/bin/python

import re
import sys
from .util import superTuple

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

def parse(schema):
    """Parse a SciDB schema into lists of attributes and dimensions.

    @description
    Returned attribute and dimension lists are "supertuples" whose
    elements can be accessed by Python attribute name.  For an
    attr_list element 'attr', you can access:

        attr.name
        attr.type
        attr.nullable

    For a dim_list element 'dim', you can access:

        dim.name
        dim.lo
        dim.hi
        dim.chunk
        dim.overlap

    If the dimension's high value was specified as '*', dim.hi will be
    sys.maxsize.

    @param schema array schema to parse
    @throws ValueError on malformed schema
    @return attr_list, dim_list
    """
    # Start by cracking schema into attributes and dimensions parts.
    m = re.match(r'\s*<([^>]+)>\s*\[([^\]]+)\]\s*$', schema)
    if not m:
        raise ValueError("bad schema: '%s'" % schema)
    # Parse attributes...
    Attribute = superTuple('Attribute', 'name', 'type', 'nullable')
    attr_descs = map(str.strip, m.group(1).split(','))
    attr_list = []
    for desc in attr_descs:
        nm, ty = map(str.strip, desc.split(':'))
        if not nm or not ty:
            raise ValueError("bad attribute: '%s'" % desc)
        ty = ty.lower()         # lowercase here so elsewhere we needn't bother
        attr_list.append(Attribute(nm, ty, 'null' in ty))
    # Parse dimensions.  Each regex match peels off a full dimension
    # spec from the left of the dimensions part.
    dim_list = []
    Dimension = superTuple('Dimension', 'name', 'lo', 'hi', 'chunk', 'overlap')
    rgx = r'\s*([^=\s]+)\s*=\s*(\d+)\s*:\s*(\*|\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*'
    dims = m.group(2)
    if not dims:
        raise ValueError("schema has no dimensions: '%s'" % schema)
    while dims:
        m = re.match(rgx, dims)
        if not m:
            raise ValueError("bad dimension(s): '%s'" % dims)
        high = sys.maxsize if m.group(3) == '*' else long(m.group(3))
        dim_list.append(Dimension(m.group(1), long(m.group(2)), high,
                                  long(m.group(4)), long(m.group(5))))
        if rgx[0] != ',':
            rgx = ',%s' % rgx   # subsequent matches need inter-group comma
        dims = dims[len(m.group(0)):]
    return attr_list, dim_list

def main(args=None):
    if args is None:
        args = sys.argv

    for arg in args[1:]:
        print '----', arg
        alist, dlist = parse(arg)
        for i, attr in enumerate(alist):
            print ' '.join(("Attribute[%d]:" % i, attr.name, attr.type,
                            "*" if attr.nullable else ""))
        for i, dim in enumerate(dlist):
            print ' '.join(map(str, ("Dimension[%d]:" % i, dim.name, dim.lo,
                                     "*" if dim.hi == sys.maxsize else dim.hi,
                                     dim.chunk, dim.overlap)))
        adict = dict([(x.name, x) for x in alist])
        if "foo" in adict:
            print "Foo is such a boring name for an attribute."

    return 0

if __name__ == '__main__':
    sys.exit(main())
