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

#include <system/SystemCatalog.h>                        // For SystemCatalog
#include <util/arena/ScopedArena.h>                      // For ScopedArena
#include <util/arena/Vector.h>                           // For mgd::vector
#include "AST.h"                                         // For Node etc.

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

/**
 *  @brief      Eliminates syntacic sugar by rewriting derived constructs into
 *              the kernel language.
 *
 *  @details    Currently handles:
 *
 *              - create array() => create_array_as()
 *
 *  @author     jbell@paradigm4.com.
 */
class Desugarer : public Visitor
{
 public:                   // Construction
                              Desugarer(Factory&,Log&);

 private:                  // From class Visitor
    virtual void              onApplication(Node*&);

 private:                  // CreateArrayUsing:
            void              onCreateArrayUsing(Node*&);
            name              caGetMi           (Node*,const ArrayDesc&)const;
            Node*             caGetEi           (Node*)                 const;
            Node*             caGetDi           (Node*)                 const;
            Node*             caGetX            (Node*)                 const;
            Node*             caGetXi           (Node*)                 const;
            Node*             caConcat          (cnodes)                const;

 private:                  // Representation
            Factory&          _fac;                      // The node factory
            Log&              _log;                      // The error log
            SystemCatalog&    _cat;                      // The system catalog
            ScopedArena       _mem;                      // The local heap
};

/**
 *
 */
    Desugarer::Desugarer(Factory& f,Log& l)
             : _fac(f),
               _log(l),
               _cat(*SystemCatalog::getInstance()),
               _mem("parser::Desugarer")
{}

/**
 *
 */
void Desugarer::onApplication(Node*& pn)
{
    assert(pn!=0 && pn->is(application));                // Validate arguments

 /* Is this a top level application of the 'create_array()' operator to five
    operands? If so, rewrite as a call to 'create_array_as()'...*/

    if (strcasecmp("Create_Array",pn->get(applicationArgOperator)->get(variableArgName)->getString())==0
                               && pn->get(applicationArgOperands)->getSize() == 5)
    {
        onCreateArrayUsing(pn);                          // ...rewrite the app
    }

    Visitor::onApplication(pn);                          // Process as before
}

/**
 *  Translate:
 *
 *      CREATE_ARRAY      (A,<..>[D1=L1:H1,C1,O1, .. , Dn=Ln:Hn,Cn,On],T,L,C)
 *
 *  into:
 *
 *      CREATE_ARRAY_USING(A,<..>[D1=L1:H1,C1,O1, .. , Dn=Ln:Hn,Cn,On],T,
 *          concat(
 *              M1(L,E1,D1),
 *                   ..
 *              Mn(L,En,Dn)),
 *          sys_distinct(L,X,C))
 *
 *  where:
 *
 *      A   = is the name of the new array to be created
 *
 *      T   = is true for a temp array and false otherwise
 *
 *      L   = is an existing 'load array' whose data is to be analysed
 *
 *      Di  = names either an attribute or dimension of L
 *
 *      Mi  = "sys_dim_a" if Di is an attribute of L
 *            "sys_dim_d" if Di is a  dimension of L
 *
 *      Ei  = is a build string of the form:
 *
 *              "[([Li,Hi,Ci,Oi)]"
 *
 *            where each component is a boolean literal, according to whether
 *            the corresponding component of the target schema is meaningful
 *            (true) or is to be inferred (false).
 *
 *      X   = "string(D1) + '|' .. '|' + string(Dn)"
 *
 *      C   = is the desired logical cell count (default = 1M)
 */
void Desugarer::onCreateArrayUsing(Node*& pn)
{
    assert(pn->is(application));                         // Validate arguments

    location    const w(pn->getWhere());                 // The source location
    Node*       const a(pn->get(applicationArgOperands));// The operand list
    Node*       const D(a->get(listArg1,schemaArgDimensions));
    Node*       const L(a->get(listArg3));               // The load array L
    Node*       const C(a->get(listArg4));               // The cell count C
    ArrayDesc         l;                                 // The schema for L
    mgd::vector<Node*>v(&_mem);                          // The args to concat

 /* Fetch the array descriptor 'l' for the load array 'L', which we will need
    in order to distinguish  whether each target  dimension 'd' is a dimension
    or attribute of the load array 'L'...*/

    _cat.getArrayDesc(L->get(referenceArgName,variableArgName)->getString(),l);

 /* For each dimension 'd' of the proposed target schema, construct (abstract
    syntax for) the initial (synthesized) arguments to the 'create_array_as()'
    operator into which we are rewriting this application...*/

    for_each (Node* d,D->getList())
    {
        name  Mi(caGetMi(d,l));                          // ...macro name
        Node* Li(_fac.newCopy(L));                       // ...load array
        Node* Ei(caGetEi(d));                            // ...build string
        Node* Di(caGetDi(d));                            // ...dimension ref

        v.push_back(_fac.newApp(w,Mi,Li,Ei,Di));         // ...apply macro
    }

 /* Rewrite the original application of 'create_array' as seen by the parser
    into a call to the the 'create_array_as' operator as described above...*/

    pn = _fac.newApp(w,"Create_Array_Using",
            caConcat(v),
			_fac.newApp(w,"sys_create_array_aux",
			    _fac.newCopy(L),                         // The load array
			    caGetX(D),                               // The distinct string
                C),                                      // The desired cells
            a->get(listArg0),                            // The target name
            a->get(listArg1),                            // The target schema
            a->get(listArg2));                           // The temporary flag
}

/**
 *  Return the name of the system macro we should use to compute statistics of
 *  the load array 'l' for the proposed target dimension'pn'.
 */
name Desugarer::caGetMi(Node* pn,const ArrayDesc& l) const
{
    assert(pn->is(dimension));                           // Validate arguments

    name n(pn->get(dimensionArgName)->getString());      // Get dimesnion name

    for_each (const DimensionDesc& d,l.getDimensions())  // For each dim of l
    {
        if (d.hasNameAndAlias(n))                        // ...do names match?
        {
            return "sys_create_array_dim";               // ....so pn is a dim
        }
    }

    return "sys_create_array_att";                       // Is attribute of l
}

/**
 *  Return a build string of the form:
 *
 *      Ei := "[([Li,Hi,Ci,Oi)]"
 *
 *  that is suitable as an argument for the 'build' operator, in which each of
 *  the components encodes whether it was specified by the user (true), or is
 *  to be computed from the load array statistics (false).
 */
Node* Desugarer::caGetEi(Node* pn) const
{
    assert(pn->is(dimension));                           // Validate arguments

    std::ostringstream os;

    os << "[(";
    os << (pn->get(dimensionArgLoBound)      !=0) << ',';
    os << (pn->get(dimensionArgHiBound)      !=0) << ',';
    os << (pn->get(dimensionArgChunkInterval)!=0) << ',';
    os << (pn->get(dimensionArgChunkOverlap) !=0);
    os << ")]";

    return _fac.newString(pn->getWhere(),
           _fac.getArena().strdup(os.str().c_str()));
}

/**
 *  Return (the abstract syntax for) a reference to the attribute or dimension
 *  'pn' of the load array.
 */
Node* Desugarer::caGetDi(Node* pn) const
{
    assert(pn->is(dimension));                           // Validate arguments

    return _fac.newRef(pn->getWhere(),pn->get(dimensionArgName));
}

/**
 *  Return (the abstract syntax for) a scalar expression of the form:
 *
 *      X := "string(D1) + '|' .. '|' + string(Dn)"
 *
 *  where the Di name the dimensions of the load array.
 */
Node* Desugarer::caGetX(Node* pn) const
{
    assert(pn->is(list));                                // Validate arguments

    location   const w(pn->getWhere());
    cnodes     const d(pn->getList());
    cnodes::iterator i(d.begin());
    Node*            p(caGetXi(*i));

    while (++i != d.end())
    {
        p = _fac.newApp   (w,"+",p,
            _fac.newApp   (w,"+",
            _fac.newString(w,"|"),
            caGetXi       (*i)));
    }

    return p;
}

/**
 *  Return (the abstract syntax for) an operator expression of the form:
 *
 *      Concat(Concat(n1,n2),n3, ...
 */
Node* Desugarer::caConcat(cnodes n) const
{
    assert(!n.empty());                                  // Validate arguments

    cnodes::iterator i(n.begin());                       //
    Node*            p(*i);                              // The first argument

    while (++i != n.end())                               // More to process?
    {
        p = _fac.newApp(p->getWhere(),"Concat",p,*i);    // ...concat another
    }

    return p;                                            // The concatenation
}

/**
 *
 */
Node* Desugarer::caGetXi(Node* pn) const
{
    assert(pn->is(dimension));                           // Validate arguments

    location const w(pn->getWhere());                    // The source location

    return _fac.newApp(w,"string",_fac.newRef(w,pn->get(dimensionArgName)));
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Traverse the abstract syntax tree in search of derived constructs that are
 *  to be rewritten into the kernel syntax.
 */
Node*& desugar(Factory& f,Log& l,Node*& n)
{
    return Desugarer(f,l)(n);                            // Run the desugarer
}

/****************************************************************************/
}}
/****************************************************************************/
