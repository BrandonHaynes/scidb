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

#include "AST.h"                                         // For Factory

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

/**
 *  @brief      Extends class Node to carry a constant datum.
 *
 *  @details    Class NodeOf  implements the boiler-plate needed to augment an
 *              abstract syntax tree node with a single constant data value.
 *
 *              The factory creates instances to represent the various literal
 *              constants that arise within the grammar of the language. These
 *              nodes are immutable, so may be freely shared amongst any other
 *              nodes that reside within the same parent tree.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class value>
class NodeOf : public Node
{
 public:                   // Construction
                              NodeOf(type t,const location& w,value v)
                               : Node(t,w,cnodes()),
                                 _value(v)               {}

 public:                   // Allocation
            void*             operator new(size_t n,Arena& a){return ::operator new(n,a,finalizer<NodeOf>());}
            void              operator delete(void*,Arena&)  {}

 public:                   // Operations
            value             getValue()           const {return _value;}
    virtual Node*             copy(Factory& f)     const {return new(f.getArena()) NodeOf(*this);}

 private:                  // Representation
            value      const  _value;                    // The constant value
};

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Construct an abstract syntax node factory that allocates memory for syntax
 *  nodes from the given arena, which in turn takes responsibility for garbage
 *  collecting the data structure as a whole when the arena is finally reset.
 */
    Factory::Factory(Arena& arena)
           : _arena(arena),
             _stack(64),
             _items(0)
{
    assert(arena.supports(resetting));                   // Garbage collecting
}

/**
 *  Allocate a node of type 't' that's associated with the source location 'w'
 *  and that carries pointers to the given children along with it.
 *
 *  Notice that:
 *
 *  1) the new node is allocated within the same arena with which this factory
 *  was itself constructed and that supports resetting, so the nodes we create
 *  are effectively garbage collected, hence have no need to implement or call
 *  destructors.
 *
 *  2) the range of child nodes is placed at the end of the node itself by the
 *  class specific allocator Node::operator new().
 *
 *  This function provides the underlying implementation for most of the other
 *  overloaded factory functions defined below.
 */
Node* Factory::newNode(type t,const location& w,cnodes children)
{
    return new(_arena,children) Node(t,w,children);      // Allocate off arena
}

/**
 *  Allocate a node of type 't' that's associated with the source location 'w'
 *  and that has no children of its own.
 */
Node* Factory::newNode(type t,const location& w)
{
    return new(_arena,cnodes()) Node(t,w,cnodes());      // Allocate off arena
}

/**
 *  Allocate and return a deep copy of the node 'n' and all its children.  The
 *  origin flag 'o' indicates in which arena the tree 'n' was first allocated:
 *  in this arena - in which case it is safe to share share immutable branches
 *  of this data structure with the new copy - or another arena, in which case
 *  we must avoid sharing any data between the two trees.
 */
Node* Factory::newCopy(const Node* n,origin o)
{
    struct copier : Visitor
    {
        copier(Factory* f,origin o)
         : _fac(f),                                      // The node factory
           _xfr(o == fromAnotherArena)                   // Transferring out?
        {}

        void onNode(Node*& pn)
        {
            if (_xfr || !pn->isEmpty())                  // Is copy necessary?
            {
                pn = pn->copy(*_fac);                    // ...copy the node

                visit(pn->getList());                    // ...visit children
            }
        }

        void onString(Node*& pn)
        {
            if (_xfr)                                    // Is copy necessary?
            {
                Arena& a = _fac->getArena();             // ...the target arena
                chars  s = a.strdup(pn->getString());    // ...copy the string
                pn = _fac->newString(pn->getWhere(),s);  // ...copy the node
            }
        }

        Factory* const _fac;                             // The node factory
        bool     const _xfr;                             // Transferring out?
    };

    if (n == 0)                                          // Nothing to copy?
    {
        return 0;                                        // ...that was easy
    }

    return copier(this,o)(const_cast<Node*&>(n));        // Run the copier
}

/**
 *  Construct a node to represent a constant null that is associated with the
 *  source location 'w'.
 *
 *  We hope to eventually support a variety of different nulls, but at present
 *  all nulls are essentially the same, so we can simply use a generic node to
 *  represent this constant.
 */
Node* Factory::newNull(const location& w)
{
    return newNode(cnull,w);                             // Return simple node
}

/**
 *  Construct a node to represent the constant string 's'. String literals are
 *  represented as raw pointers to characters allocated in the caller's arena;
 *  this scheme generally works well because most of the strings in the syntax
 *  tree originate from within the lexer, but for strings generated within the
 *  translator it is sometimes easier to work with string objects, and then we
 *  must copy the strings into the arena first before wrapping the raw pointer
 *  with a node object as before.
 */
Node* Factory::newString(const location& w,string s)
{
    return newString(w,_arena.strdup(s));                // Copy into new node
}

/**
 *  Construct a node to represent the lambda abstraction:
 *  @code
 *      fn (<formal_1> , .. , <formal_n>) { <body> }
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newAbs(const location& w,Node* formals,Node* body)
{
    assert(formals!=0 && body!=0);                       // Validate arguments

    return newNode(abstraction,w,formals,body);          // Create abstraction
}

/**
 *  Construct a node to represent the trivial application expression:
 *  @code
 *      <name> ( )
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newApp(const location& w,name name)
{
    return newApp(w,name,cnodes());                      // Create application
}

/**
 *  Construct a node to represent the application expression:
 *  @code
 *      <name> ( operand[1] , .. , operand[n] )
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newApp(const location& w,name name,cnodes operands)
{
    return newApp(w,newString(w,name),operands);         // Create application
}

/**
 *  Construct a node to represent the application expression:
 *  @code
 *      <name> ( operand[1] , .. , operand[n] )
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newApp(const location& w,Name* name,cnodes operands)
{
    assert(name!=0 && name->is(cstring));                // Validate arguments

    Node* v = newVar(w,name);                            // The operator expr
    Node* l = newNode(list,w,operands);                  // The operands list

    return newNode(application,w,v,l,0);                 // Create application                                           // ...no alias here
}

/**
 *  Construct a node to represent the recursive let binding expression:
 *  @code
 *      fix { <binding_1> ; .. ; <binding_n>) } in <body>
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newFix(const location& w,Node* bindings,Node* body)
{
    assert(bindings!=0 && body!=0);                      // Validate arguments

    if (bindings->isEmpty())                             // No actual bindings?
    {
        return body;                                     // ...a special case
    }

    return newNode(fix,w,bindings,body);                 // Create fix binding
}

/**
 *  Construct a node to represent the non-recursive let binding expression:
 *  @code
 *      let { <binding_1> ; .. ; <binding_n>) } in <body>
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newLet(const location& w,Node* bindings,Node* body)
{
    assert(bindings!=0 && body!=0);                      // Validate arguments

    if (bindings->isEmpty())                             // No actual bindings?
    {
        return body;                                     // ...a special case
    }

    return newNode(let,w,bindings,body);                 // Create let binding
}

/**
 *  Construct a node to represent the  occurrence of either an array qualified
 *  or version qualified reference within an expression:
 *  @code
 *      <av> . <name> <order>                            // Array qualified
 *   or
 *      <name> [ <av> [ <order> ] ]                      // Version qualified
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newRef(const location& w,Name* name,Node* av,Node* order)
{
    assert(name!=0 && name->is(cstring));                // Validate arguments

    Node* n = newVar(name->getWhere(),name);             // The reference name
    Node* a = 0;                                         // Array   qualifier
    Node* v = 0;                                         // Version qualifier

    if (av!=0 && av->is(cstring))                        // Has an array name?
    {
        a = newVar(av->getWhere(),av);                   // ...set qualifier
    }
    else                                                 // Missing array name
    {
        v = av;                                          // ...set qualifier
    }

    return newNode(reference,w,n,a,v,order,0);           // Create a reference
}

/**
 *  Construct a node to represent the occurrence of an unqualified name within
 *  an expression:
 *  @code
 *      <name>
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newVar(const location& w,name name)
{
    assert(name != 0);                                   // Validate arguments

    return newNode(variable,w,newString(w,name),0);      // Create a variable
}

/**
 *  Construct a node to represent the occurrence of an unqualified name within
 *  an expression:
 *  @code
 *      <name>
 *  @endcode
 *  that is associated with the location 'w' in the original source text.
 */
Node* Factory::newVar(const location& w,Name* name)
{
    assert(name!=0 && name->is(cstring));                // Validate arguments

    return newNode(variable,w,name,0);                   // Create a variable
}

/**
 *  Allocate a node of type 'list' that is associated with the source location
 *  'w' and that carries pointers to the 'items' children currently sitting at
 *  the top of the parser shadow stack.
 */
Node* Factory::newList(const location& w,size_t items)
{
    return newNode(list,w,pop(items));                   // Pop shadow stack
}

/**
 *  Push the given node onto the top of the parser shadow stack.
 */
void Factory::push(Node* node)
{
    if (_items < _stack.size())                          // Sufficient space?
    {
        _stack[_items] = node;                           // ...write onto top
    }
    else                                                 // Sorry, there isn't
    {
        _stack.push_back(node);                          // ...so grow vector
    }

    _items++;                                            // Adjust stack top

    assert(_items <= _stack.size());                     // Check consistency
}

/**
 *  Pop the given number of nodes from the parser shadow stack and return them
 *  in a pointer range.
 */
cnodes Factory::pop(size_t items)
{
    assert(items <= _items);                             // Validate arguments

    _items -= items;                                     // Pop the items off

    return cnodes(items,&_stack[_items]);                // But do not resize
}

/****************************************************************************/

Node* Factory::newReal   (const location& w,real    v){return new(_arena) NodeOf<real>   (creal,   w,v);}
Node* Factory::newString (const location& w,chars   v){return new(_arena) NodeOf<chars>  (cstring, w,v);}
Node* Factory::newBoolean(const location& w,boolean v){return new(_arena) NodeOf<boolean>(cboolean,w,v);}
Node* Factory::newInteger(const location& w,integer v){return new(_arena) NodeOf<integer>(cinteger,w,v);}

/****************************************************************************/

real    Node::getReal()   const {assert(is(creal));   return downcast<const NodeOf<real>*>   (this)->getValue();}
chars   Node::getString() const {assert(is(cstring)); return downcast<const NodeOf<chars>*>  (this)->getValue();}
boolean Node::getBoolean()const {assert(is(cboolean));return downcast<const NodeOf<boolean>*>(this)->getValue();}
integer Node::getInteger()const {assert(is(cinteger));return downcast<const NodeOf<integer>*>(this)->getValue();}

/** @cond ********************************************************************
 * We use the preprocessor to automatically create the remaining overloads..*/
#define SCIDB_NEW_NODE(_,i,__)                                                 \
                                                                               \
Node* Factory::newNode(type t,const location& w,BOOST_PP_ENUM_PARAMS(i,Node*n))\
{                                                                              \
    Node* c[] = {BOOST_PP_ENUM_PARAMS(i,n)};                                   \
                                                                               \
    return newNode(t,w,c);                                                     \
}                                                                              \
                                                                               \
Node* Factory::newApp(const location& w,name n,BOOST_PP_ENUM_PARAMS(i,Node*n)) \
{                                                                              \
    Node* c[] = {BOOST_PP_ENUM_PARAMS(i,n)};                                   \
                                                                               \
    return newApp(w,n,c);                                                      \
}                                                                              \

BOOST_PP_REPEAT_FROM_TO(1,8,SCIDB_NEW_NODE,"")           // Emit the overloads
#undef SCIDB_NEW_NODE                                    // And clean up after
/** @endcond ****************************************************************/
}}
/****************************************************************************/
