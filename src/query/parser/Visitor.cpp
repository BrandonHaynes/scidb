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

#include "AST.h"                                         // For Visitor

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

/**
 *  Visit in situ each non-null node within the given range, possibly changing
 *  it as we go, and return the updated range of nodes.
 */
nodes Visitor::visit(nodes nodes)
{
    for_each (Node*& n,nodes)                            // For each node 'n'
    {
        if (n != 0)                                      // ...is proper node?
        {
            this->visit(n);                              // ....visit in place
        }
    }

    return nodes;                                        // The updated range
}

/**
 *  Visit the given node in place by testing its type tag and then dispatching
 *  to the appropriate virtual function. This gives our subclass the chance to
 *  examine the node and perhaps modify it or even replace it altogether.
 */
Node*& Visitor::visit(Node*& pn)
{
    assert(pn != 0);                                     // Validate arguments

    switch (pn->getType())                               // What kind of node?
    {
        default:          this->onNode       (pn);  break;// ...basic node
        case abstraction: this->onAbstraction(pn);  break;// ...abstraction
        case application: this->onApplication(pn);  break;// ...application
        case fix:         this->onFix        (pn);  break;// ...fix expression
        case let:         this->onLet        (pn);  break;// ...let expression
        case reference:   this->onReference  (pn);  break;// ...reference
        case schema:      this->onSchema     (pn);  break;// ...schema
        case variable:    this->onVariable   (pn);  break;// ...variable
        case cnull:       this->onNull       (pn);  break;// ...null
        case creal:       this->onReal       (pn);  break;// ...real
        case cstring:     this->onString     (pn);  break;// ...string
        case cboolean:    this->onBoolean    (pn);  break;// ...boolean
        case cinteger:    this->onInteger    (pn);  break;// ...integer
        case module:      this->onModule     (pn);  break;// ...module
        case insertArray: this->onInsertArray(pn);  break;// ...insert array
        case selectArray: this->onSelectArray(pn);  break;// ...select array
        case updateArray: this->onUpdateArray(pn);  break;// ...update array
        case binding:     this->onBinding    (pn);  break;// ...binding
        case attribute:   this->onAttribute  (pn);  break;// ...attribute
        case dimension:   this->onDimension  (pn);  break;// ...dimension
    }

    return pn;                                           // The processed node
}

/**
 *  Visit the given node.
 */
void Visitor::onNode(Node*& pn)
{
    assert(pn != 0);                                     // Validate arguments
}

/**
 *  Visit the given expression.
 */
void Visitor::onExpression(Node*& pn)
{
    assert(pn != 0);                                     // Validate arguments

    this->onNode(pn);                                    // Visit 'base class'
}

/**
 *  Visit the given abstraction expression.
 */
void Visitor::onAbstraction(Node*& pn)
{
    assert(pn!=0 && pn->is(abstraction));                // Validate arguments

    this->onExpression(pn);                              // Visit 'base class'
}

/**
 *  Visit the given application expression.
 */
void Visitor::onApplication(Node*& pn)
{
    assert(pn!=0 && pn->is(application));                // Validate arguments

    this->onExpression(pn);                              // Visit 'base class'
}

/**
 *  Visit the given fix expression.
 */
void Visitor::onFix(Node*& pn)
{
    assert(pn!=0 && pn->is(fix));                        // Validate arguments

    this->onExpression(pn);                              // Visit 'base class'
}

/**
 *  Visit the given let expression.
 */
void Visitor::onLet(Node*& pn)
{
    assert(pn!=0 && pn->is(let));                        // Validate arguments

    this->onExpression(pn);                              // Visit 'base class'
}

/**
 *  Visit the given reference expression.
 */
void Visitor::onReference(Node*& pn)
{
    assert(pn!=0 && pn->is(reference));                  // Validate arguments

    this->onExpression(pn);                              // Visit 'base class'
}

/**
 *  Visit the given schema expression.
 */
void Visitor::onSchema(Node*& pn)
{
    assert(pn!=0 && pn->is(schema));                     // Validate arguments

    this->onExpression(pn);                              // Visit 'base class'
}

/**
 *  Visit the given variable expression.
 */
void Visitor::onVariable(Node*& pn)
{
    assert(pn!=0 && pn->is(variable));                   // Validate arguments

    this->onExpression(pn);                              // Visit 'base class'
}

/**
 *  Visit the given constant expression.
 */
void Visitor::onConstant(Node*& pn)
{
    assert(pn != 0);                                     // Validate arguments

    this->onExpression(pn);                              // Visit 'base class'
}

/**
 *  Visit the given null constant expression.
 */
void Visitor::onNull(Node*& pn)
{
    assert(pn!=0 && pn->is(cnull));                      // Validate arguments

    this->onConstant(pn);                                // Visit 'base class'
}

/**
 *  Visit the given real constant expression.
 */
void Visitor::onReal(Node*& pn)
{
    assert(pn!=0 && pn->is(creal));                      // Validate arguments

    this->onConstant(pn);                                // Visit 'base class'
}

/**
 *  Visit the given string constant expression.
 */
void Visitor::onString(Node*& pn)
{
    assert(pn!=0 && pn->is(cstring));                    // Validate arguments

    this->onConstant(pn);                                // Visit 'base class'
}

/**
 *  Visit the given boolean constant expression.
 */
void Visitor::onBoolean(Node*& pn)
{
    assert(pn!=0 && pn->is(cboolean));                   // Validate arguments

    this->onConstant(pn);                                // Visit 'base class'
}

/**
 *  Visit the given integral constant expression.
 */
void Visitor::onInteger(Node*& pn)
{
    assert(pn!=0 && pn->is(cinteger));                   // Validate arguments

    this->onConstant(pn);                                // Visit 'base class'
}

/**
 *  Visit the given statement.
 */
void Visitor::onStatement(Node*& pn)
{
    assert(pn != 0);                                     // Validate arguments

    this->onNode(pn);                                    // Visit 'base class'
}

/**
 *  Visit the given module statement.
 */
void Visitor::onModule(Node*& pn)
{
    assert(pn!=0 && pn->is(module));                     // Validate arguments

    this->onStatement(pn);                               // Visit 'base class'
}

/**
 *  Visit the given insert array statement.
 */
void Visitor::onInsertArray(Node*& pn)
{
    assert(pn!=0 && pn->is(insertArray));                // Validate arguments

    this->onStatement(pn);                               // Visit 'base class'
}

/**
 *  Visit the given select array statement.
 */
void Visitor::onSelectArray(Node*& pn)
{
    assert(pn!=0 && pn->is(selectArray));                // Validate arguments

    this->onStatement(pn);                               // Visit 'base class'
}

/**
 *  Visit the given update array statement.
 */
void Visitor::onUpdateArray(Node*& pn)
{
    assert(pn!=0 && pn->is(updateArray));                // Validate arguments

    this->onStatement(pn);                               // Visit 'base class'
}

/**
 *  Visit the given variable binding.
 */
void Visitor::onBinding(Node*& pn)
{
    assert(pn!=0 && pn->is(binding));                    // Validate arguments

    this->onNode(pn);                                    // Visit 'base class'
}

/**
 *  Visit the given array attribute.
 */
void Visitor::onAttribute(Node*& pn)
{
    assert(pn!=0 && pn->is(attribute));                  // Validate arguments

    this->onNode(pn);                                    // Visit 'base class'
}

/**
 *  Visit the given array dimension.
 */
void Visitor::onDimension(Node*& pn)
{
    assert(pn!=0 && pn->is(dimension));                  // Validate arguments

    this->onNode(pn);                                    // Visit 'base class'
}

/****************************************************************************/
}}
/****************************************************************************/
