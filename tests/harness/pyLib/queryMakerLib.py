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
class defaultBuildQuery(object):
    def __init__(
        self,
        a=['attr1'],
        t=['double'],
        d=['i','j'],
        r=[(0,99),(0,99)],
        c=[4,4],
        o=[0,0],
        i='random()',
        n=False
        ):
        self.__kwargs = {
            'a': a,
            't': t,
            'd': d,
            'r': r,
            'c': c,
            'o': o,
            'i': i,
            'n': n
            }
        
    def schema(self,**kwargs):
        # Validate keyword arguments:
        if (not (all([x in self.__kwargs.keys() for x in kwargs.keys()]))):
            print 'Unknown keyword arguments in defaultBuildQuery.build.'
            return ''
        kw = self.__kwargs.copy()
        kw.update(kwargs)
        
        attrNames = kw['a']
        attrTypes = kw['t']
        dims = kw['d']
        ranges = kw['r']
        chunks = kw['c']
        overlaps = kw['o']
        nullable = kw['n']
        
        schemaStatement = '<attrs>[dims]'
        
        if (len(attrNames) > len(attrTypes)):
            attrTypes.extend([attrTypes[-1] for i in range(len(attrNames) - len(attrTypes))])
            
        if (len(dims) > len(ranges)):
            ranges.extend([ranges[-1] for i in range(len(dims) - len(ranges))])
            
        if (len(dims) > len(chunks)):
            chunks.extend([chunks[-1] for i in range(len(dims) - len(chunks))])
            
        if (len(dims) > len(overlaps)):
            overlaps.extend([overlaps[-1] for i in range(len(dims) - len(overlaps))])
        
        attrList = [x[0] + ':' + x[1] + ',' for x in zip(attrNames,attrTypes)]
        attrList[-1] = attrList[-1][:-1]
        
        if (nullable):
            attrList.append(' null')
        
        dimList = [
            x[0] + '=' + str(x[1][0]) + ':' + str(x[1][1]) + ',' + str(x[2]) + ',' + str(x[3]) + ',' for x in zip(dims,ranges,chunks,overlaps)
            ]
        dimList[-1] = dimList[-1][:-1]
        
        schemaStatement = schemaStatement.replace('attrs',''.join(attrList))
        schemaStatement = schemaStatement.replace('dims',''.join(dimList))
        
        return schemaStatement
        
    def build(self,**kwargs):
        buildStatement = 'build(schema,init)'
        sch = self.schema(**kwargs)
        
        kw = self.__kwargs.copy()
        kw.update(kwargs)
        
        init = kw['i']
        
        buildStatement = buildStatement.replace('schema',sch)
        buildStatement = buildStatement.replace('init',str(init))
        
        return buildStatement
'''        
if __name__ == '__main__':
    dbq = defaultBuildQuery()
    print dbq.schema()
    print dbq.schema(a=['attr1','attr2'])
    print dbq.schema(a=['attr3'])
    print dbq.schema(a=['attr1','attr2'],t=['int64'],i='iif(i=j,null,1)')
    print dbq.schema(a=['attr1','attr2','attr3'])
    print dbq.schema(c=[6,6])
    print dbq.schema(o=[6,6])
    print dbq.schema(n=True)
'''
