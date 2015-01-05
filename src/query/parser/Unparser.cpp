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

#include "AST.h"                                         // For Node etc.

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

/**
 *  @brief      Format the tree as source code and write it to a stream.
 *
 *  @details    Class Unparser implements a specialized tree traversal visitor
 *              that visits each node of the abstract syntax tree,  formatting
 *              the nodes as source code that it inserts onto an output stream
 *              it carries with it on its journey around the tree.
 *
 *              This class provides the underlying implementation for the node
 *              stream insertion operator.
 *
 *  @author     jbell@paradigm4.com.
 */
class Unparser : public Visitor
{
 public:                   // Construction
                              Unparser(ostream& o)
                               : _out(o)                 {}

 private:                  // From class Visitor
    virtual void              onAbstraction(Node*&);
    virtual void              onApplication(Node*&);
    virtual void              onFix        (Node*&);
    virtual void              onLet        (Node*&);
    virtual void              onReference  (Node*&);
    virtual void              onSchema     (Node*&);
    virtual void              onVariable   (Node*&);
    virtual void              onNull       (Node*&);
    virtual void              onReal       (Node*&);
    virtual void              onString     (Node*&);
    virtual void              onBoolean    (Node*&);
    virtual void              onInteger    (Node*&);
    virtual void              onModule     (Node*&);
    virtual void              onBinding    (Node*&);
    virtual void              onAttribute  (Node*&);
    virtual void              onDimension  (Node*&);

 private:                  // Implementation
            void              join         (Node*,const char* = "");

 private:                  // Representation
            ostream&          _out;                      // The output stream
};

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onAbstraction(Node*& pn)
{
    assert(pn!=0 && pn->is(abstraction));                // Validate arguments

    _out << "fn(";                                       // fn (
    join (pn->get(abstractionArgBindings),",");          //     a,..,z
    _out << "){";                                        //           ) {
    visit(pn->get(letArgBody));                          //     <body>
    _out << '}';                                         //             }
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onApplication(Node*& pn)
{
    assert(pn!=0 && pn->is(application));                // Validate arguments

    visit(pn->get(applicationArgOperator));              // <id>
    _out << '(';                                         //     (
    join (pn->get(applicationArgOperands),",");          //      <a>,..,<z>
    _out << ')';                                         //                )

    if (Node* n = pn->get(applicationArgAlias))          // Has an alias name?
    {
        _out << " as " << n->getString();                // ...emit the alias
    }
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onFix(Node*& pn)
{
    assert(pn!=0 && pn->is(fix));                        // Validate arguments

    _out << "fix {";                                     // fix {
    join (pn->get(fixArgBindings),";");                  //      <id> = ...
    _out << "} in ";                                     //     } in
    visit(pn->get(fixArgBody));                          //           <body>
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onLet(Node*& pn)
{
    assert(pn!=0 && pn->is(let));                        // Validate arguments

    _out << "let {";                                     // let {
    join (pn->get(letArgBindings),";");                  //      <id> = ...
    _out << "} in ";                                     //     } in
    visit(pn->get(letArgBody));                          //          <body>
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onReference(Node*& pn)
{
    assert(pn!=0 && pn->is(reference));                  // Validate arguments

    if (Node* n = pn->get(referenceArgArray))            // Has an array name?
    {
        visit(n);                                        // ...visit qualifier
        _out << '.';                                     // ...append the '.'
    }

    visit(pn->get(referenceArgName));                    // Visit the name

    if (Node* n = pn->get(referenceArgVersion))          // Has version stamp?
    {
        _out << '@';                                     // ...append the '@'
        visit(n);                                        // ...visit version
    }

    if (Node* n = pn->get(referenceArgOrder))            // Has an orderering?
    {
        _out << ' ' << order(n->getInteger());           // ...emit the order
    }

    if (Node* n = pn->get(referenceArgAlias))            // Has an alias name?
    {
        _out << " as " << n->getString();                // ...emit the alias
    }
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onSchema(Node*& pn)
{
    assert(pn!=0 && pn->is(schema));                     // Validate arguments

    _out << '<';                                         // <
    join (pn->get(schemaArgAttributes),",");             //   a1:t1 .. an:tn
    _out << '>';                                         // >
    _out << '[';                                         // [
    join (pn->get(schemaArgDimensions),",");             //   d1=b1 .. dn=bn
    _out << ']';                                         // ]
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onVariable(Node*& pn)
{
    assert(pn!=0 && pn->is(variable));                   // Validate arguments

    _out << pn->get(variableArgName)->getString();       // Emit variable name
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onNull(Node*& pn)
{
    assert(pn!=0 && pn->is(cnull));                      // Validate arguments

    _out << "null";                                      // Emit the constant
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onReal(Node*& pn)
{
    assert(pn!=0 && pn->is(creal));                      // Validate arguments

    _out << pn->getReal();                               // Emit the constant
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onString(Node*& pn)
{
    assert(pn!=0 && pn->is(cstring));                    // Validate arguments

    _out << '\'' << pn->getString() << '\'';             // Emit the constant
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onBoolean(Node*& pn)
{
    assert(pn!=0 && pn->is(cboolean));                   // Validate arguments

    _out << boolalpha << pn->getBoolean();               // Emit the constant
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onInteger(Node*& pn)
{
    assert(pn!=0 && pn->is(cinteger));                   // Validate arguments

    _out << pn->getInteger();                            // Emit the constant
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onModule(Node*& pn)
{
    assert(pn!=0 && pn->is(module));                     // Validate arguments

    join (pn->get(moduleArgBindings),";\n");             // <id> = ... ; ...
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onBinding(Node*& pn)
{
    assert(pn!=0 && pn->is(binding));                    // Validate arguments

    _out << pn->get(bindingArgName)->getString();        // <id>

    if (pn->has(bindingArgBody))                         // Has a proper body?
    {
        _out << " = ";                                   //       =
        visit(pn->get(bindingArgBody));                  //         <body>
     }
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onAttribute(Node*& pn)
{
    assert(pn!=0 && pn->is(attribute));                  // Validate arguments

    _out << pn->get(attributeArgName)->getString();      // Insert the name
    _out << ':';                                         // Insert delimiter
    _out << pn->get(attributeArgTypeName)->getString();  // Insert the type

    if (Node* n = pn->get(attributeArgIsNullable))       // Has nullable flag?
    {
        _out << (n->getBoolean() ? " null" : "");        // ...emit nullable
    }

    if (Node* n = pn->get(attributeArgDefaultValue))     // Has default value?
    {
        _out << " default " << n;                        // ...emit default
    }

    if (Node* n = pn->get(attributeArgCompressorName))   // Has a compressor?
    {
        _out << " compression " << n;                    // ...emit compressor
    }

    if (Node* n = pn->get(attributeArgReserve))          // Has reserve size?
    {
        _out << " reserve " << n;                        // ...emit reserve
    }
}

/**
 *  Insert a formatted representation of the node 'pn' onto our output stream.
 */
void Unparser::onDimension(Node*& pn)
{
    assert(pn!=0 && pn->is(dimension));                  // Validate arguments

    _out << pn->get(dimensionArgName)->getString();      // Emit the name

    if (Node* n = pn->get(dimensionArgLoBound))          // Has lower bound?
    {
        _out << '=' << n;                                // ...emit the bound
    }
    else                                                 // No, its missing
    {
        _out << '=' << 0;                                // ...emit default
    }

    if (Node* n = pn->get(dimensionArgHiBound))          // Has upper bound?
    {
        _out << ':' << n;                                // ...emit the bound
    }
    else                                                 // No, its missing
    {
        _out << ":*";                                    // ...emit default
    }

    if (Node* n = pn->get(dimensionArgChunkInterval))    // Has an interval?
    {
        _out << ',' << n;                                // ...emit interval
    }
    else                                                 // No, its missing
    {
        _out << ",*";                                    // ...emot default
    }

    if (Node* n = pn->get(dimensionArgChunkOverlap))     // Has an overlap?
    {
        _out << ',' << n;                                // ...emit overlap
    }
    else                                                 // No, its missing
    {
        _out << ",0";                                    // ...emit default
    }
}

/**
 *  Insert a formatted representation of each node in the list 'pn' in turn on
 *  our output stream, separating each with the given delimiter string.
 */
void Unparser::join(Node* pn,const char* delimiter)
{
    assert(pn!=0 && pn->is(list) && delimiter!=0);       // Validate arguments

    insertRange(_out,pn->getList(),delimiter);           // Emit the bindings
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Insert a formatted representation of the ordering 'order' onto the output
 *  stream 'io'.
 */
ostream& operator<<(ostream& io,order order)
{
    return io << (order==ascending ? "asc" : "desc");    // Emit the ordering
}

/**
 *  Insert a formatted representation of the node 'pn' onto the output stream
 *  'io'.
 */
ostream& operator<<(ostream& io,const Node* pn)
{
    return Unparser(io)(const_cast<Node*&>(pn)), io;     // Run the unparser
}

/**
 *  Return a string representation of the node 'pn' formatted as source code.
 */
string unparse(const Node* pn)
{
    ostringstream o;                                     // Make local stream
    o << pn;                                             // Drop  onto stream
    return o.str();                                      // And return string
}

/****************************************************************************/
}}
/****************************************************************************/
