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

#include "AST.h"                                         // For Node

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

/**
 *  Complete the construction for a node of type 't', associated with location
 *  'w' in the original source text, whose children are specified in the range
 *  'c'.
 *
 *  Our specialised 'new' operator has cunningly arranged for additional space
 *  to be allocated at the end of the object, and has even copied the children
 *  into this special space for us, so all that remains for us to do now is to
 *  initialize our fixed size data members, as per usual.
 */
    Node::Node(type t,const location& w,cnodes c)
        : _type(t),
          _where(w),
          _size(c.size())
{}

/**
 *  Allocate the underlying memory for a node with children 'c'.  We are using
 *  the old C programmer's trick of extending the allocation to save the array
 *  at the end of the node because a) this saves space, but, more importantly,
 *  b)  by not carrying a container, our destructor need no longer be invoked.
 *  As a result, the entire abstract syntax tree can be cheaply created in the
 *  caller's resetting arena and simply flushed in one go once the translation
 *  is complete: there is no need to recurse back over the tree just to invoke
 *  destructors.
 */
void* Node::operator new(size_t n,Arena& a,cnodes c)
{
    void* p = a.malloc(n + c.size() * sizeof(Node*));    // Room for children
    Node* q = static_cast<Node*>(p);                     // Start of the node

    const_cast<size_t&>(q->_size) = c.size();            // Assign node arity
    std::copy(c.begin(),c.end(),q->getList().begin());   // Copy childen over

    return p;                                            // Return allocation
}

/**
 *  Allocate and return a shallow copy of this node; that is, a new allocation
 *  whose contents are identical to this one, and that, in particular, carries
 *  pointers to exactly the same children as we do.
 */
Node* Node::copy(Factory& f) const
{
    return new(f.getArena(),getList()) Node(*this);      // Allocate new copy
}

/**
 * Return a string representation of the node formatted as source text.
 *
 * Don't expect to be able to parse the resulting string: the function is only
 * provided to facilitate inspecting from within a debugger.
 */
string Node::dump() const
{
    ostringstream o;                                     // New string stream
    o << this;                                           // Drop node into it
    return o.str();                                      // And return string
}

/**
 *  Return any optional alias that may be associated with the expression 'pn'.
 *
 *  Aliases are currently represented as part of the syntactic structure of an
 *  operator application or array reference;  what would make more sense would
 *  be to treat the alias as a binary operator that endows Any expression with
 *  an optional alias (which can subsequently be ignored, perhaps).
 */
Name* getAlias(const Node* pn)
{
    if (pn->is(application))                             // Is an application?
    {
        return pn->get(applicationArgAlias);             // ...retrieve alias
    }
    else
    if (pn->is(reference))                               // Is it a reference?
    {
        return pn->get(referenceArgAlias);               // ...retrieve alias
    }

    return 0;                                           // No, it has no alias
}

/**
 *  Assign the alias 'pa' to the expression 'pn'.
 *
 *  Aliases are currently represented as part of the syntactic structure of an
 *  operator application or array reference;  what would make more sense would
 *  be to treat the alias as a binary operator that endows Any expression with
 *  an optional alias (which can subsequently be ignored, perhaps).
 */
Node*& setAlias(Node*& pn,Name* pa)
{
    assert(pn != 0);                                     // Validate the node
    assert(pa==0 || pa->is(cstring));                    // Validate the alias

    if (pn->is(application))                             // Is an application?
    {
        pn->set(applicationArgAlias,pa);                 // ...assign an alias
    }
    else
    if (pn->is(reference))                               // Is it a reference?
    {
        pn->set(referenceArgAlias,pa);                   // ...assign an alias
    }
    else                                                 // Is something else?
    {
                                                         // ...just ignore it
    }

    return pn;                                           // Return parent node
}

/****************************************************************************/
}}
/****************************************************************************/
