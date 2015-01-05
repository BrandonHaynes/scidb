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

#include <util/arena/ScopedArena.h>                      // For ScopedArena
#include <util/arena/Vector.h>                           // For mgd::vector
#include "Table.h"                                       // For Table
#include "AST.h"                                         // For Node etc.

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

/**
 *  @brief      Performs inline expansion of locally bound abstractions.
 *
 *  @details    Class Inliner descends recursively through a tree in search of
 *              applications of locally bound abstractions - scoped macros, in
 *              other words - and indiscriminately expands these inline at the
 *              call sites. The result is a sort of naive macro expansion.
 *
 *              The current implementation has a few limitations, however:
 *
 *              - Our runtime  execution mechanism currently lacks support for
 *              both closures and named temporaries, hence we *must* eliminate
 *              *all* let and lambda expressions here; there is simply nothing
 *              available for us to compile them into if we do not.
 *
 *              - This in turn precludes the possibility of handling recursive
 *              functions. The AFL 'let' binding construct, which we represent
 *              internally using the 'fix' AST type, has an intended semantics
 *              that mirrors Haskell's 'let' and Scheme's 'letrec' constructs.
 *              In the absence of such execution support, however,a reasonable
 *              compromise seems to be to detect attempts at recursion - which
 *              we throw out with a 'not yet supported' sort of message - then
 *              compile what remains with a 'let*' type semantics; that is, as
 *              a list of successively nested non-recursive 'let' expressions.
 *              This strategy is compatable with the final intended semantics,
 *              so enables us to add support for recursion in a future release
 *              without altering the meaning of those programs that succeed in
 *              compiling now.
 *
 *              - The AST data structure currently has no means of storing the
 *              results of a binding analysis: variables are instead looked up
 *              every time they are encountered; this should be addressed in a
 *              future release.  In the meantime, however, it is uneccessarily
 *              difficult and expensive to determine the set of free variables
 *              of an expression on the fly and, as a result, the substitution
 *              method employed here will potentially capture variables. This,
 *              too, should be addressed in a subsequent release.
 *
 *              Class Inliner is implemented as a Visitor;  it carries with it
 *              a linked list of Table's that collectively model the notion of
 *              a current lexical environment 'E'. Visiting a node 'n' has the
 *              effect of rewriting it in place with the result of calculating
 *              the substitution 'n [ E ]', which, for macros, achieves a beta
 *              reduction.
 *
 *  @note       The notation:
 *
 *                  [[ t ]]
 *
 *              denotes the translation of a term 't' effected by running this
 *              visitor recursively over it.  The [[ ]] are variously known as
 *              'Oxford', 'Scott', or 'semantic', brackets.
 *
 *              The notation:
 *
 *                  t [ n1 := b1 ; .. ; nn := bn ]
 *
 *              denotes the (possibly capturing) substitution of bindings 'bi'
 *              for free occurrences of  the names 'ni' encountered within the
 *              term 't'. The [ ] are known as 'substitution brackets'. Within
 *              the brackets we list the elements of a set of pairs 'ni := bi'
 *              that define a finite mapping from names to terms,  referred to
 *              collectively as an 'environment'.
 *
 *              The notation:
 *
 *                  E1 ; E2
 *
 *              denotes the environment obtained by extending E1 with elements
 *              drawn from E2 in such a way that the bindings of E2 hide those
 *              of E1. In other words:
 *
 *                  n [ E1 ; E2 ]  =  n [ E2 ]  if n:=t is in E2
 *                                 =  n [ E2 ]  if n:=t is in E1 but not E2
 *                                 =  n [    ]  otherwise
 *
 *  @see        http://www.haskell.org/onlinereport/exps.html#3.12 for more on
 *              Haskell's 'let' construct, upon which our 'let' is modelled.
 *
 *  @see        http://sicp.ai.mit.edu/Fall-2004/manuals/scheme-7.5.5/doc/scheme_3.html#SEC31
 *              for more on Scheme's 'let', 'letrec' and 'let*' constructs.
 *
 *  @see        http://en.wikipedia.org/wiki/Lambda_calculus for a definition
 *              of substitution in the context of the lambda calculus.
 *
 *  @author     jbell@paradigm4.com.
 */
class Inliner : public Visitor
{
 public:                   // Construction
                              Inliner(Factory&,Log&);

 private:                  // From class Visitor
    virtual void              onNode       (Node*&);
    virtual void              onAbstraction(Node*&);
    virtual void              onApplication(Node*&);
    virtual void              onFix        (Node*&);
    virtual void              onLet        (Node*&);
    virtual void              onReference  (Node*&);
    virtual void              onModule     (Node*&);

 private:                  // Implementation
            Node*             getBody(const Name*) const;
            void              enter(const Node*);
            void              leave();

 private:                  // Representation
            Factory&          _fac;                      // The node factory
            Log&              _log;                      // The error log
            ScopedArena       _mem;                      // The local heap
            Table*            _tbl;                      // The local table
            Node*       const _nil;                      // The 'nil' constant
};

/**
 *  Construct an Inliner that allocates tree nodes using the factory 'f', that
 *  reports errors to the log 'l', and that allocates any other memory it will
 *  be needing from the private resetting arena '_mem'.
 *
 *  The local table '_tbl' keeps track of the current lexical environment;  we
 *  initialize it with an empty table - one that contains no bindings at all -
 *  and push and pop additional frames as needed on entry to and exit from the
 *  nested scopes introduced by the various binding constructs we encounter on
 *  our journey around the abstract syntax tree.
 *
 *  The node 'nil' is a special constant we create, distinct from any constant
 *  that the user could come by, that we shall temporarily assign to the right
 *  hand side of 'fix' bindings in order to detect attempts at recursion.
 */
    Inliner::Inliner(Factory& f,Log& l)
           : _fac(f),
             _log(l),
             _mem("parser::Inliner"),
             _tbl(getTable()),
             _nil(Factory(_mem).newNull(location()))
{}

/**
 *  Visit each child of the node 'pn'.
 *
 *  This 'catch all' function ensures that the visitor object will be taken to
 *  every tree node, even those with types for which we have not implemented a
 *  specialized virtual function.
 */
void Inliner::onNode(Node*& pn)
{
    assert(pn != 0);                                     // Validate arguments

    visit(pn->getList());                                // Visit the children
}

/**
 *  Translate the lambda abstraction:
 *
 *      fn ( n1 , .. , nn ) { t }
 *
 *  as
 *
 *      fn ( n1 , .. , nn ) { t [ n1 := 0 ; .. ; nn := 0 ] }
 *
 *  The formal parameters 'ni' are represented as 'binding' nodes whose bodies
 *  are set to 0,  encoding the fact that these variables are to be considered
 *  'bound' within the body of the lambda abstraction, and therefore immune to
 *  further substitution; the function getBody() is aware of this encoding.
 */
void Inliner::onAbstraction(Node*& pn)
{
    assert(pn!=0 && pn->is(abstraction));                // Validate arguments

    enter(pn->get(abstractionArgBindings));              // Enter new scope
    visit(pn->get(abstractionArgBody));                  // Visit the body
    leave();                                             // Leave the scope
}

/**
 *  Translate the application expression:
 *
 *      m ( o1 , .. , on )
 *
 *  as
 *
 *      t [ E ; n1 := [[o1]] ; .. ; nn = [[on]] ]
 *
 *  if 'm' is locally bound to an abstraction of the form:
 *
 *      fn ( n1 , .. , nn ) { t }
 *
 *  and as
 *
 *      m ( [[o1]] , .. , [[on]] )
 *
 *  otherwise.
 */
void Inliner::onApplication(Node*& pn)
{
    assert(pn!=0 && pn->is(application));                // Validate arguments

    Name* n(pn->get(applicationArgOperator));            // Fetch the operator
    nodes o(pn->get(applicationArgOperands)->getList()); // Fetch the operands

    visit(o);                                            // Assign oi = [[oi]]

    if (Node* m = getBody(n))                            // Has local binding?
    {
        if (!m->is(abstraction))                         // ...to a non-macro?
        {
            _log.fail(SCIDB_LE_NAME_NOT_APPLICABLE,*n);  // ....log this error
            return;                                      // ....leave it alone
        }

        if (m->get(abstractionArgBindings)->getSize() != o.size())
        {
            _log.fail(SCIDB_LE_NAME_ARITY_MISMATCH,*n);  // ....log this error
            return;                                      // ....leave it alone
        }

        Node* const a = getAlias(pn);                    // ...save any alias

        m  = _fac.newCopy(m);                            // ...copy the macro
        pn = m->get(abstractionArgBody);                 // ...take its body
        m  = m->get(abstractionArgBindings);             // ...and its formals

        setAlias(pn,_fac.newCopy(a));                    // ...apply the alias

     /* Build the extension n1:=o1 ; .. ; nn:=on of the lexical environment E
        by rebinding copies of 'm's formal parameters in place...*/

        for (size_t i=0,n=o.size(); i!=n; ++i)           // ...for each formal
        {
            Node*  bi = m->get(child(i));                // ....fetch binding

            assert(bi->is(binding));                     // ....verify type
            assert(bi->get(bindingArgBody) == 0);        // ....verify unbound

            bi->set(bindingArgBody,o[i]);                // ....assign operand
        }

     /* Enter the new bindings into the environment, and visit a copy of 'm's
        body, so replacing 'pn' with the result of computing the substitution
        pn [E ; n1:=o1 ; .. ; nn:=on] ...*/

        enter(m);                                        // ...enter new scope
        visit(pn);                                       // ...visit the body
        leave();                                         // ...leave the scope
    }
}

/**
 *  Translate the recursive fix expression:
 *
 *      fix { n1 = b1 ; .. ; nn = bn } in t
 *
 *  as
 *
 *      [[ let { n1 = nil ; .. ; nn = nil } in
 *         let { n1 = b1 } in
 *           ..
 *         let { nn = bn } in t ]]
 *
 *  As is explained above, this falls short of the intended recursive semantic
 *  that is modelled on Haskell's 'let' construct,  but agrees with it (modulo
 *  the topological sorting of the bindings that a proper stratification would
 *  accomplish) in those cases where no actual recursion is present - that is,
 *  no free occurrences of the 'ni' in any of the {b1,..,bn, t} turn out to be
 *  bound to 'nil'.
 *
 *  Rather than build the above translation literally, however, it is simpler,
 *  and slightly more efficient, to simulate the translation as follows:
 *
 *  First, we construct the environment E ; n1:=nil ; .. ; nn:=nil, that binds
 *  each name 'ni' being introduced to the special constant 'nil', taking care
 *  to save each 'bi' in the vector 'pairs' where we can retrieve it later.
 *
 *  We then visit each right hand side 'bi' in turn; if any such body 'bi' has
 *  a free occurence of one of the 'nj' this will be detected as an attempt to
 *  reference the constant 'nil', and we have spotted an attempt at recursion.
 *  If not, we may now safely repair the binding ni:=bi so that the subsequent
 *  expressions can refer to it.
 *
 *  Thus we endow the fix expression with a 'let*' type semantics while never-
 *  theless reserving the right to interpret this construct as being recursive
 *  in a future release.
 */
void Inliner::onFix(Node*& pn)
{
    assert(pn!=0 && pn->is(fix));                        // Validate arguments

    typedef pair<Node*,Node*> pair;                      // (ni := bi , bi)
    mgd::vector<pair>         pairs(&_mem);              // The saved pairs

 /* Record each binding 'b = ni:=bi' before rebinding the 'ni' to 'nil'...*/

    for_each (Node* b,pn->getList(fixArgBindings))       // For each b=ni:=bi
    {
        pairs.push_back(pair(b,b->get(bindingArgBody))); // ...record (b,bi)
        b->set(bindingArgBody,_nil);                     // ...rebind to nil
    }

 /* Enter the bindings n1:=nil; .. ; nn:=nil into the environment...*/

    enter(pn->get(fixArgBindings));                      // Enter new scope

 /* Process each of the bodies 'bi' that we saved earlier in 'pairs' together
    with the modified bindings to which they belong; if we make it through to
    the other side, then the 'bi' is not recursive, and we may now repair the
    binding 'b = ni := bi'...*/

    for_each (pair& p,pairs)                             // For each (ni:=bi,bi)
    {
        visit(p.second);                                 // ...process body bi
        p.first->set(bindingArgBody,p.second);           // ...restore binding
    }

 /* Process the fix body in the environment E ; n1:=b1; .. ; nn:=bn ...*/

    visit(pn->get(fixArgBody));                          // Process fix body
    leave();                                             // Leave the scope

 /* Discard the enclosing 'fix': the body now *is* the result we want...*/

    pn  = pn->get(fixArgBody);                           // Replace with body
}

/**
 *  Translate the non-recursive let expression:
 *
 *      let { n1 = b1 ; .. ; nn = bn } in t
 *
 *  as
 *
 *      t [ n1 = [[b1]] ; .. ; nn = [[bn]] ]
 *
 *  In other words, process each of the local bindings 'bi' recursively, then
 *  substitute them for free occurrences of the variables 'ni' within 't'.
 *
 *  Notice that the 'ni' are not entered  into the environment until After the
 *  the 'bi' are processed, giving the 'let' AST node a simple, non-recursive,
 *  semantics, which occasionally proves useful when translating certain other
 *  constructs of  the language.  Recall that - somewhat confusingly - the AFL
 *  'let' construct is represented as a 'fix' AST node, whereas the 'let' node
 *  node currently has no such direct syntactic counterpart - it exists purely
 *  to assist in the translation of certain other constructs: chiefly, when we
 *  wish to bind new names to subexpressions without fear of variable capture.
 */
void Inliner::onLet(Node*& pn)
{
    assert(pn!=0 && pn->is(let));                        // Validate arguments

 /* Visit each of the 'bi' in the current environment 'E'...*/

    for_each (Node* b,pn->getList(letArgBindings))       // For each binding
    {
        visit(b->get(bindingArgBody));                   // ...visit its body
    }

 /* Visit the body in the extended environment E ; n1:=b1 ; .. ; nn:=bn...*/

    enter(pn->get(letArgBindings));                      // Enter new scope
    visit(pn->get(letArgBody));                          // Visit the body
    leave();                                             // Leave the scope

 /* Discard the enclosing 'let': the body now *is* the result we want...*/

    pn  = pn->get(letArgBody);                           // Replace with body
}

/**
 *  Translate the reference expression:
 *
 *      name
 *
 *  to its binding in the current substitution - if, in fact, it has one - but
 *  leave it alone if it is array-qualified, since the latter must necessarily
 *  refer to a global entity.
 *
 *  Notice how we transfer any optional alias that may be associated with this
 *  reference over to the expression to which the reference refers.
 */
void Inliner::onReference(Node*& pn)
{
    assert(pn!=0 && pn->is(reference));                  // Validate arguments

    if (pn->has(referenceArgArray))                      // Is name qualified?
    {
        return;                                          // ...then its global
    }

    if (Node* n = getBody(pn->get(referenceArgName)))    // Has local binding?
    {
        n = _fac.newCopy(n);                             // ...reuse the body

        if (const Name* a = pn->get(referenceArgAlias))  // ...was it aliased?
        {
            setAlias(n,_fac.newCopy(a));                 // ....transfer alias
        }

        pn = n;                                          // ...update argument
    }
}

/**
 *  Translate the module statement:
 *
 *      n1 = b1 ; .. ; nn = bn
 *
 *  as if it were simply an expression of the form:
 *
 *      fix { n1 = b1 ; .. ; nn = bn } in nil;
 *
 *  and discard the resulting body when done.
 *
 *  This completely captures the semantics of the current module statement. In
 *  future, however, we will want to add other syntactic bells and whistles to
 *  the module statement in order to specify, for example, the module imports,
 *  exports, namespace aliases, and so on.
 */
void Inliner::onModule(Node*& pn)
{
    assert(pn!=0 && pn->is(module));                     // Validate arguments

    Node* b = pn->get(moduleArgBindings);                // n1:=b1 ;..; nn:=bn
    Node* n = _fac.newFix(pn->getWhere(),b,_nil);        // fix {ni=bi} in nil

    visit(n);                                            // Now visit bindings
}

/**
 *  Search the local table for a binding of the given name and return the node
 *  to which it is currently bound, or zero otherwise. If bound to the special
 *  constant '_nil', throw an error instead; we have detected a reference to a
 *  binding whose processing has not yet been completed, an indication that it
 *  may be recursively defined.
 *
 *  In other words:
 *
 *      fail        if E is .. ; name := nil; ..
 *      return t    if E is .. ; name := t  ; ..
 *      return 0    otherwise
 *
 *  for some term 't' other than 'nil'.
 *
 *  Notice that lambda-bound names - those introduced as the formal parameters
 *  of a lambda abstraction - are represented as nodes of type 'binding' whose
 *  'body' is null; thus although appearing in the local table (and so failing
 *  to be 'free') nevertheless have no actual binding available, hence neither
 *  are they candidates for replacement under the current substitution.
 */
Node* Inliner::getBody(const Name* name) const
{
    assert(name!=0 && name->is(variable));               // Validate arguments

    name = name->get(variableArgName);                   // Get variable name

    if (const Node* b = _tbl->get(name))                 // Has local binding?
    {
        assert(b->is(binding));                          // ...verify its type

        if (b->get(bindingArgBody) == _nil)              // ...is incomplete?
        {
            _log.fail(SCIDB_LE_NAME_IS_RECURSIVE,*name); // ....log the error
        }

        return b->get(bindingArgBody);                   // ...return its body
    }

    return 0;                                            // No local binding
}

/**
 *  Enter a new scope, extending the current lexical environment 'E' with the
 *  given bindings n1:=b1 ; .. ; nn:=bn, which temporarily hide any others of
 *  the same name. In other words, we construct the environment:
 *
 *      E ; n1:=b1 ; .. ; nn:=bn
 *
 *  We implement the 'current lexical environment' as a linked list of tables,
 *  the topmost such table being accessed via the '_tbl' data member, and push
 *  another table onto the front of this list now.
 */
void Inliner::enter(const Node* bindings)
{
    assert(bindings!=0 && bindings->is(list));           // Validate arguments

    _tbl = newTable(_mem,_log,_tbl,bindings->getList()); // Push another table
}

/**
 *  Leave the current scope, retracting the definitions of those bindings that
 *  were temporarily brought into scope by the previous call to enter().
 *
 *  We pop the top-most table from the stack;  notice that there is no need to
 *  destroy it as it was originally allocated on our private resetting arena.
 */
void Inliner::leave()
{
    assert(_tbl != 0);                                   // Validate our state

    _tbl = _tbl->getParent();                            // Pop top-most table

    assert(_tbl != 0);                                   // Validate our state
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Traverse the abstract syntax tree in search of locally bound variables and
 *  indiscriminately substitute them inline to implement a naive sort of macro
 *  expansion facility.
 */
Node*& inliner(Factory& f,Log& l,Node*& n)
{
    return Inliner(f,l)(n);                              // Run the inliner
}

/****************************************************************************/
}}
/****************************************************************************/
