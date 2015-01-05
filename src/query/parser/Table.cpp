/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/****************************************************************************/

#include <util/arena/Map.h>                              // For mgd::map
#include "Table.h"                                       // For Table
#include "AST.h"                                         // For Node

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

struct byName
{
    bool operator()(name a,name b) const                 {return strcmp(a,b)<0;}
};

typedef mgd::map<name,const Node*,byName> map;           // Map names to Nodes

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Construct a table implementation that  represents a local extension of the
 *  environment described with the table 'parent', extended to include each of
 *  the bindings in the given range.
 *
 *  The resulting table is allocated within the given resetting arena,which is
 *  now responsible for disposal of the table, although not its contents, when
 *  the arena is eventually reset.
 */
Table* newTable(Arena& arena,Log& log,Table* parent,cnodes bindings)
{
    assert(parent!=0 && arena.supports(resetting));      // Validate arguments

    struct table : Table, map
    {
     /* Construct a local extension of the parent table 'p' by hiding the
        inherited bindings with those in the given range...*/

        table(Arena& a,Log& l,Table* p,cnodes bindings)
          : map(&a),
            _tbl(p)
        {
            for_each (const Node* b,bindings)            // For each binding
            {
                add(l,b);                                // ...add to the map
            }
        }

    /*  Return the number of bindings in this frame of the symbol table...*/

        size_t size() const
        {
            return map::size();                          // Defer to our base
        }

     /* Return the parent table with which we were constructed...*/

        Table* getParent() const
        {
            return _tbl;                                 // Our parent table
        }

     /* Return the node to which the given name is bound. We search our own
        local bindings with a lookup in the map, and if no such binding is,
        delegate to our parent. Eventually this chain of delegation ends up
        in the currently loaded module...*/

        Node const* get(const Name* name) const
        {
            assert(name != 0);                           // Validate arguments

            const_iterator i = find(name->getString());  // Look for the name

            if (i != end())                              // Found it in there?
            {
                return i->second;                        // ...return binding
            }
            else                                         // No, don't have it
            {
                return _tbl->get(name);                  // ...defer to parent
            }
        }

     /* Add the given binding to our local map, but complain if the name is
        already in there, as might happen, for example, if the user were to
        attempt to define a macro with two parameters of the same name...*/

        void add(Log& log,const Node* bind)
        {
            assert(bind!=0 && bind->is(binding));        // Validate arguments

            const Name* n(bind->get(bindingArgName));    // Fetch binding name
            value_type  v(n->getString(),bind);          // Value to insert

            if (!insert(v).second)                       // Failed to insert?
            {
                log.fail(SCIDB_LE_NAME_REDEFINED,*n);    // ...report an error
            }
        }

     /* Apply the given visitor to each binding in the table...*/

        void accept(Visitor& visit) const
        {
            for (const_iterator i=begin(),e=end(); i!=e; ++i)
            {
                visit(const_cast<Node*&>(i->second));    // ...visit binding
            }
        }

        Table* const _tbl;                               // The parent table
    };

    return new(arena) table(arena,log,parent,bindings);  // Create local table
}

/****************************************************************************/
}}
/****************************************************************************/
