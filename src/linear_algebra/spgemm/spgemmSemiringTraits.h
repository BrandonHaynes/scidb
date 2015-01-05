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
#ifndef SPGEMM_SEMIRING_TRAITS_H_
#define SPGEMM_SEMIRING_TRAITS_H_

/*
 * DCSRBlock.h
 *
 *  Created on: November 4, 2013
 */


// boost
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

// local
#include "SpAccumulator.h"

namespace scidb
{
// Abstract Semiring concept:
//
// A class serving as SemiringTraits_tt for the DCSRBlock::spgemm<SemiringTraits_tt>().
//
// In the mathematical abstract, this provides the information of the mathematical structure
// called a semiring. (see, e.g. http://en.wikipedia.org/wiki/Semiring).
//
// In the programming abstract (a concept), a class must provide five typedefs corresponding to the five
// mathematical parts of a semiring: (S, +, x, 0, 1) which are, respectively,
// the carrier set, addition operator, multiplication operator, additive identity, and multiplicative identity.
// The typedefs are: Value_t, OpAdd_t, OpMult_t, IdAdd_t, and IdMult_t.
//
// Value_tt : type being used to hold a member of the semiring's set.  This is the
//              argument and return type of the semiring's binary operators.
//              it corresponds to the the semiring set, S

// Value_tt OpAdd_tt::operate(Value_t,Value_t) must implement the semiring addition
// Value_tt IdAdd_tt::value() must return the semiring's additive identity.
// these two classes represent the abstract addition and additive identity of the semiring's commutative monoid, (S,+,0)
//
// Value_tt OpMut_tt::operate(Value_t,Value_t) must implement the semiring multiplication
// Value_tt IdMul_tt::value() must return the semiring's multiplicative identity.
// these two classes represent the abstract multiplication and multiplicative identity of the semiring's group (S,*,1)
//

// here's a "standard" helper template for making such a class:
template<class Value_tt, class OpAdd_tt, class OpMul_tt, class IdAdd_tt, class IdMul_tt>
struct SemiringTraits {
    typedef Value_tt                    Value_t ;
    typedef OpAdd_tt                    OpAdd_t ;
    typedef OpMul_tt                    OpMul_t ;
    typedef IdAdd_tt                    IdAdd_t ;
    typedef IdMul_tt                    IdMul_t ;
};


//
// for use as template arguments to SemiringTraits, above
// (from within SemiRingTraits{PlusStar,MinPlus,MaxPlus}{ZeroOne,InfZero,MinfZero}
//
template<class Value_tt>
struct OperatorPlus {
    static Value_tt operate(Value_tt valA, Value_tt valB) {
        return valA + valB ;
    }
};
template<class Value_tt>
struct OperatorStar {
    static Value_tt operate(Value_tt valA, Value_tt valB) {
        return valA * valB ;
    }
};
template<class Value_tt>
struct OperatorStdMin {
    static Value_tt operate(Value_tt valA, Value_tt valB) {
        return std::min(valA, valB);
    }
};
template<class Value_tt>
struct OperatorStdMax {
    static Value_tt operate(Value_tt valA, Value_tt valB) {
        return std::max(valA, valB);
    }
};
template<class Value_tt>
struct OperatorOne {
    static Value_tt operate(Value_tt valA, Value_tt valB) {
        return 1;
    }
};


template<class Value_tt>
struct Zero
{
    static Value_tt value() { return 0; }
};

template<class Value_tt>
struct One
{
    static Value_tt value() { return 1; }
};

template<class Value_tt>
struct Infinity
{
    static Value_tt value() { return std::numeric_limits<Value_tt>::infinity(); }
};

template<class Value_tt>
struct MInfinity
{
    static Value_tt value() { return - std::numeric_limits<Value_tt>::infinity(); }
};


// template typedef for implementing arithmetic semirings with fixed identities over different types
template<class Value_tt>
struct SemiringTraitsPlusStarZeroOne
:
    SemiringTraits<Value_tt, OperatorPlus<Value_tt>, OperatorStar<Value_tt>, Zero<Value_tt>, One<Value_tt> >
{};


// template typedef for implementing the min.+ "tropical" semiring over different types
template<class Value_tt>
struct SemiringTraitsMinPlusInfZero
:
    SemiringTraits<Value_tt, OperatorStdMin<Value_tt>, OperatorPlus<Value_tt>, Infinity<Value_tt>, Zero<Value_tt> >
{};

// template typedef for implementing the max.+ "tropical" semiring over different types
template<class Value_tt>
struct SemiringTraitsMaxPlusMInfZero
:
    SemiringTraits<Value_tt, OperatorStdMax<Value_tt>, OperatorPlus<Value_tt>, MInfinity<Value_tt>, Zero<Value_tt> >
{};

// template typedef for implementing flop counting [not a semiring ] over different types
// since its not a semiring, its possible that for different ways of doing the multiplication will result
// in different flop counts. (e.g. flops are O(m^3) for gemm(), O(m^2.8) for Strassen's algorithm.
template<class Value_tt>
struct SemiringTraitsCountMultiplies
:
    SemiringTraits<Value_tt, OperatorPlus<Value_tt>, OperatorOne<Value_tt>, Zero<Value_tt>, Zero<Value_tt> >
{};

} // end namespace scidb
#endif // SPGEMM_SEMIRING_TRAITS_H_
