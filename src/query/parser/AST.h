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

#ifndef QUERY_PARSER_AST_H_
#define QUERY_PARSER_AST_H_

/****************************************************************************/

#include "ParserDetails.h"                               // For implementation
#include "location.hh"                                   // For location

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

enum type
{
 // Expressions:

    abstraction,
    application,
    fix,
    let,
    reference,
    schema,
    variable,
    olapAggregate,

 // Constants:

    cnull,
    creal,
    cstring,
    cboolean,
    cinteger,

 // Statements:

    module,
    insertArray,
    selectArray,
     namedExpr,
     groupByClause,
     joinClause,
     regridClause,
     regridDimension,
     redimensionClause,
     thinClause,
     thinDimension,
     windowClause,
     windowDimensionRange,
    updateArray,
     update,

 // Miscellaneous:

    binding,
    attribute,
    dimension,
    asterisk,
    list
};

enum child
{
 // abstraction
    abstractionArgBindings              = 0,
    abstractionArgBody,

 // application
    applicationArgOperator              = 0,
    applicationArgOperands,
    applicationArgAlias,

 // fix
    fixArgBindings                      = 0,
    fixArgBody,

 // let
    letArgBindings                      = 0,
    letArgBody,

 // reference
    referenceArgName                    = 0,
    referenceArgArray,
    referenceArgVersion,
    referenceArgOrder,
    referenceArgAlias,

 // schema
    schemaArgAttributes                 = 0,
    schemaArgDimensions,

 // variable
    variableArgName                     = 0,
    variableArgBinding,

 // olapAggregate
    olapAggregateArgApplication         = 0,
    olapAggregateArgPartitionName,

 // module
    moduleArgBindings                   = 0,

 // insertArray
    insertArrayArgDestination           = 0,
    insertArrayArgSource,
    insertArrayArgCount,

 // updateArray
    updateArrayArgArrayRef              = 0,
    updateArrayArgUpdateList,
    updateArrayArgWhereClause,

 // update
    updateArgName                       = 0,
    updateArgExpr,

 // selectArray
    selectArrayArgSelectList            = 0,
    selectArrayArgIntoClause,
    selectArrayArgFromClause,
    selectArrayArgFilterClause,
    selectArrayArgGRWClause,
    selectArrayArgOrderByClause,

 // namedExpr
    namedExprArgExpr                    = 0,
    namedExprArgName,

 // groupByClause
    groupByClauseArgList                = 0,

 // joinClause
    joinClauseArgLeft                   = 0,
    joinClauseArgRight,
    joinClauseArgExpr,

 // regridClause
    regridClauseArgDimensionsList       = 0,

 // regridDimension
    regridDimensionArgName              = 0,
    regridDimensionArgStep,

 // thinClause
    thinClauseArgArrayReference         = 0,
    thinClauseArgDimensionsList,

 // thinDimension
    thinDimensionClauseArgName          = 0,
    thinDimensionClauseArgStart,
    thinDimensionClauseArgStep,

 // windowClause
    windowClauseArgName                 = 0,
    windowClauseArgRangesList,
    windowClauseArgVariableWindowFlag,

 // windowDimensionRange
    windowDimensionRangeArgName         = 0,
    windowDimensionRangeArgPreceding,
    windowDimensionRangeArgFollowing,

 // windowDimensionCurrent
    windowDimensionCurrentArgName       = 0,

 // binding
    bindingArgName                      = 0,
    bindingArgBody,

 // attribute
    attributeArgName                    = 0,
    attributeArgTypeName,
    attributeArgIsNullable,
    attributeArgDefaultValue,
    attributeArgCompressorName,
    attributeArgReserve,

 // dimension
    dimensionArgName                    = 0,
    dimensionArgLoBound,
    dimensionArgHiBound,
    dimensionArgChunkInterval,
    dimensionArgChunkOverlap,

 // list
    listArg0                            = 0,
    listArg1,
    listArg2,
    listArg3,
    listArg4
};

enum order
{
    ascending,                                           // Ascending  order
    descending                                           // Descending order
};

enum instances
{
    thisInstance = -2,                                   // On this instance
    allInstances = -1,                                   // On every instance
    coordinator  =  0                                    // On the coordinator
};

enum origin
{
    fromTheSameArena,                                    // For internal copy
    fromAnotherArena                                     // For external copy
};

/**
 *  @brief      Represents one node in the abstract syntax tree representation
 *              of a parsed query.
 *
 *  @see        http://en.wikipedia.org/wiki/Abstract_syntax_tree for a solid
 *              introduction to the concept of an abstract syntax tree.
 *
 *  @author     jbell@paradigm4.com.
 */
class Node
{
 public:                   // Operations
            bool              is(type t)           const {return t == _type;}
            bool              has(child c)         const {return get(c) != 0;}
            bool              isEmpty()            const {return _size == 0;}
            type              getType()            const {return _type;}
            size_t            getSize()            const {return _size;}
            location          getWhere()           const {return _where;}
            cnodes            getList()            const {return cnodes(_size,(Node**)(this+1));}
            cnodes            getList(child c)     const {return get(c)->getList();}
            Node*             get(child c)         const {return getList()[c];}
            Node*             get(child a,child b) const {return get(a)->get(b);}
            string            dump()               const;

 public:                   // Operations
            real              getReal()            const;
            chars             getString()          const;
            boolean           getBoolean()         const;
            integer           getInteger()         const;

 public:                   // Operations
            nodes             getList()                  {return nodes(_size,(Node**)(this+1));}
            nodes             getList(child c)           {return get(c)->getList();}
            Node*&            get(child c)               {return getList()[c];}
            Node*&            get(child a,child b)       {return get(a)->get(b);}
            Node*             set(child c,Node* n)       {getList()[c] = n;return this;}

 protected:                // Construction
                              Node   (type,const location&,cnodes);
            void*             operator new  (size_t,Arena&,cnodes);
            void              operator delete(void*,Arena&,cnodes) {}
    virtual Node*             copy (Factory&)      const;// Create shallow copy
            friend            class Factory;             // Manages allocation

 protected:                // Representation
            type        const _type;                     // The type of node
            location    const _where;                    // The source location
            size_t      const _size;                     // The number of kids
};

/**
 *  @brief      Creates the nodes of an abstract syntax tree.
 *
 *  @author     jbell@paradigm4.com
 */
class Factory
{
 public:                   // Construction
                              Factory(Arena&);

 public:                   // Node Construction
            Arena&            getArena()           const {return _arena;}

 public:                   // Node Construction
            Node*             newNode   (type,const location&);
            Node*             newNode   (type,const location&,Node*);
            Node*             newNode   (type,const location&,Node*,Node*);
            Node*             newNode   (type,const location&,Node*,Node*,Node*);
            Node*             newNode   (type,const location&,Node*,Node*,Node*,Node*);
            Node*             newNode   (type,const location&,Node*,Node*,Node*,Node*,Node*);
            Node*             newNode   (type,const location&,Node*,Node*,Node*,Node*,Node*,Node*);
            Node*             newNode   (type,const location&,Node*,Node*,Node*,Node*,Node*,Node*,Node*);
            Node*             newNode   (type,const location&,cnodes);
            Node*             newCopy   (const Node*,origin = fromTheSameArena);

 public:                   // Constant Construction
            Node*             newNull   (const location&);
            Node*             newReal   (const location&,real);
            Node*             newString (const location&,chars);
            Node*             newString (const location&,string);
            Node*             newBoolean(const location&,boolean);
            Node*             newInteger(const location&,integer);

 public:                   // Helper Functions
            Node*             newApp    (const location&,name);
            Node*             newApp    (const location&,name,Node*);
            Node*             newApp    (const location&,name,Node*,Node*);
            Node*             newApp    (const location&,name,Node*,Node*,Node*);
            Node*             newApp    (const location&,name,Node*,Node*,Node*,Node*);
            Node*             newApp    (const location&,name,Node*,Node*,Node*,Node*,Node*);
            Node*             newApp    (const location&,name,Node*,Node*,Node*,Node*,Node*,Node*);
            Node*             newApp    (const location&,name,Node*,Node*,Node*,Node*,Node*,Node*,Node*);
            Node*             newApp    (const location&,name,Node*,Node*,Node*,Node*,Node*,Node*,Node*,Node*);
            Node*             newApp    (const location&,name, cnodes);
            Node*             newApp    (const location&,Name*,cnodes);
            Node*             newAbs    (const location&,Node* bindings,Node* body);
            Node*             newFix    (const location&,Node* bindings,Node* body);
            Node*             newLet    (const location&,Node* bindings,Node* body);
            Node*             newRef    (const location&,Name*,Node* av = 0,Node* order = 0);
            Node*             newVar    (const location&,Name*);
            Node*             newVar    (const location&,name);
            Node*             newList   (const location&,size_t);

 public:                   // Shadow Stack
            void              push(Node*);               // Push shadow stack
            cnodes            pop(size_t);               // Pop  shadow stack

 private:                  // Representation
            Arena&            _arena;                    // Memory allocator
            vector<Node*>     _stack;                    // Parser shadow stack
            size_t            _items;                    // Top of shadow stack
};

/**
 *  @brief      Visits the nodes of an abstract syntax tree.
 *
 *  @see        http://en.wikipedia.org/wiki/Visitor_pattern for a description
 *              of the vistor design pattern.
 *
 *  @author     jbell@paradigm4.com
 */
class Visitor
{
 public:                   // Construction
    virtual                  ~Visitor()                  {}

 public:                   // Operations
            nodes             operator()   (nodes r)     {return visit(r);}
            Node*&            operator()   (Node*&n)     {return visit(n);}

 public:                   // Operations
    virtual nodes             visit        (nodes );
    virtual Node*&            visit        (Node*&);

 protected:                // Nodes
    virtual void              onNode       (Node*&);

 protected:                // Expressions
    virtual void              onExpression (Node*&);
    virtual void              onAbstraction(Node*&);
    virtual void              onApplication(Node*&);
    virtual void              onFix        (Node*&);
    virtual void              onLet        (Node*&);
    virtual void              onReference  (Node*&);
    virtual void              onSchema     (Node*&);
    virtual void              onVariable   (Node*&);

 protected:                // Constants
    virtual void              onConstant   (Node*&);
    virtual void              onNull       (Node*&);
    virtual void              onReal       (Node*&);
    virtual void              onString     (Node*&);
    virtual void              onBoolean    (Node*&);
    virtual void              onInteger    (Node*&);

 protected:                // Statements
    virtual void              onStatement  (Node*&);
    virtual void              onModule     (Node*&);
    virtual void              onInsertArray(Node*&);
    virtual void              onSelectArray(Node*&);
    virtual void              onUpdateArray(Node*&);

 protected:                // Miscellaneous
    virtual void              onBinding    (Node*&);
    virtual void              onAttribute  (Node*&);
    virtual void              onDimension  (Node*&);
};

/****************************************************************************/
ostream&                      operator<<(ostream&,order);
ostream&                      operator<<(ostream&,const Node*);
/****************************************************************************/
Name*                         getAlias  (const Node*);
Node*&                        setAlias  (Node*&,Name*);
/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
