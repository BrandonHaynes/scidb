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

#include <boost/assign/list_of.hpp>                      // For list_of()
#include <array/TupleArray.h>                            // For TupleArray
#include "Table.h"                                       // For Table
#include "AST.h"                                         // For Node

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace boost;                                   // All of boost
using namespace assign;                                  // For list_of()
using namespace parser;                                  // ALl of parser

/****************************************************************************/

/**
 *  Implements the 'inferSchema' method for the "list('macros')" operator.
 */
ArrayDesc logicalListMacros()
{
    size_t const n = max(getTable()->size(),1LU);        // Elements to emit

    return ArrayDesc("macros",                           // The array name
        list_of(AttributeDesc(0,"name",TID_STRING,0,0))  // ...name attribute
               (AttributeDesc(1,"type",TID_STRING,0,0)), // ...type attribute
        list_of(DimensionDesc("No",0,n-1,n,0)));         // Has one dimension
}

/**
 *  Implements the 'execute' method for the "list('macros')" operator.
 *
 *  We define a local visitor subclass that formats each binding it visits and
 *  pushes another tuple onto the end of the vector it carries along with it.
 */
shared_ptr<Array> physicalListMacros(const ArenaPtr& arena)
{
    struct Lister : Visitor
    {
        Lister(const Table& t,const ArenaPtr& arena)
         : tuples(make_shared<TupleArray>(logicalListMacros(),arena))
        {
            t.accept(*this);                             // Visit the bindings
        }

        void onBinding(Node*& pn)
        {
            Value t[2];                                  // Local value pair

            t[0].setString(getName(pn));                 // Set name component
            t[1].setString(getType(pn));                 // Set type component

            tuples->appendTuple(t);                      // Append the tuple
        }

        name getName(const Node* pn)
        {
            return pn->get(bindingArgName)->getString(); // The macro name
        }

     /* Format and return a type string of the form 'name(a1, .. ,aN)', where
        the idenitifiers 'a.i' name the formal parameters of the macro...*/

        string getType(const Node* pn)
        {
            ostringstream s;                             // Local out stream

            s << getName(pn);                            // Insert macro name

            pn = pn->get(bindingArgBody);                // Aim at macro body

            if (pn->is(abstraction))                     // Takes parameters?
            {
                pn = pn->get(abstractionArgBindings);    // ...aim at bindings
                s << '(';                                // ...opening paren
                insertRange(s,pn->getList(),',');        // ...join with ','
                s << ')';                                // ...closing paren
            }

            return s.str();                              // The type string
        }

        shared_ptr<TupleArray> tuples;                   // The result array
    };

    return Lister(*getTable(),arena).tuples;             // Run the lister
}

/****************************************************************************/
}
/****************************************************************************/
