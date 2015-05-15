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


/*
 * @file TileFunctions.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Templates of implementation tile functions
 */

#ifndef TILEFUNCTIONS_H
#define TILEFUNCTIONS_H

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <array/Metadata.h>
#include <array/RLE.h>

namespace scidb
{

struct VarValue
{
    boost::shared_ptr<Value> value;
    boost::shared_ptr< std::vector<char> > varPart;
};

/**
 * A set of helper templates for code generation related to adding and setting values
 */
template <typename T>
size_t addPayloadValues(RLEPayload* p, size_t n = 1)
{
    return p->addRawValues(n);
}

template <> inline
size_t addPayloadValues<bool>(RLEPayload* p, size_t n)
{
    return p->addBoolValues(n);
}

template <> inline
size_t addPayloadValues<VarValue>(RLEPayload* p, size_t n)
{
    // XXX HACK alert!
    // Note the size is set to 0 because we just want to know
    // the next index for value insertion without growing
    // the internal datastructure.
    // setPayloadValue() will do it for us.
    return p->addRawVarValues(0);
}

template <typename T>
void setPayloadValue(RLEPayload* p, size_t index, T value) {
    T* data = (T*)p->getRawValue(index);
    *data = value;
}

// Specialization for bool
template <> inline
void setPayloadValue<bool>(RLEPayload* p, size_t index, bool value) {
    char* data = p->getFixData();
    if (value) {
        data[index >> 3] |= 1 << (index & 7);
    } else {
        data[index >> 3] &= ~(1 << (index & 7));
    }
}

template <> inline
void setPayloadValue<VarValue>(RLEPayload* p, size_t index, VarValue value)
{
    p->appendValue(*value.varPart, *value.value, index);
}

template <typename T>
T getPayloadValue(const ConstRLEPayload* p, size_t index) {
    return *(T*)p->getRawValue(index);
}
// Specialization for bool
template <> inline
bool getPayloadValue<bool>(const ConstRLEPayload* p, size_t index) {
    return p->checkBit(index);
}

template <> inline
VarValue getPayloadValue<VarValue>(const ConstRLEPayload* p, size_t index) {
    VarValue res;
    res.value = boost::shared_ptr<Value>(new Value());
    p->getValueByIndex(*res.value, index);
    return res;
}

// UNARY TEMPLATE FUNCTIONS

template<typename T, typename TR>
struct UnaryMinus
{
    static TR func(T v)
    {
        return -v;
    }
};

template<typename T, typename TR>
struct UnaryFunctionCall
{
    typedef TR (*FunctionPointer)(T);
    template<FunctionPointer F>
    struct Function
    {
        template<typename T1, typename TR1>
        struct Op
        {
            static TR1 func(T1 v)
            {
                return F(v);
            }
        };
    };
};

template<typename T, typename TR>
struct UnaryConverter
{
    static TR func(T v)
    {
        return v;
    }
};

/**
 * Template of function for unary operations.
 */
template<template <typename T, typename TR> class O, typename T, typename TR>
void rle_unary_func(const Value** args,  Value* result, void*)
{
    const Value& v = *args[0];
    Value& res = *result;
    res.getTile()->clear();
    res.getTile()->assignSegments(*v.getTile());
    const size_t valuesCount = v.getTile()->getValuesCount();
    addPayloadValues<TR>(res.getTile(), valuesCount);
    size_t i = 0;
    T* s = (T*)v.getTile()->getFixData();
    T* end = s + valuesCount;
    while (s < end) {
        setPayloadValue<TR>(res.getTile(), i++, O<T, TR>::func(*s++));
    }
}

void rle_unary_bool_not(const Value** args,  Value* result, void*);
// BINARY TEMPLATE FUNCTIONS

template<typename T1, typename T2, typename TR>
struct BinaryPlus
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 + v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryMinus
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 - v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryMult
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 * v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryDiv
{
    static TR func(T1 v1, T2 v2)
    {
        if (0 == v2)
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIVISION_BY_ZERO);
        return v1 / v2;
    }
};

// Specialization for double and float data type to avoid error and producing NaN values according to IEEE-754
template<>
struct BinaryDiv<double, double, double>
{
    static double func(double v1, double v2)
    {
        return v1 / v2;
    }
};

template<>
struct BinaryDiv<float, float, float>
{
    static float func(float v1, float v2)
    {
        return v1 / v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryMod
{
    static TR func(T1 v1, T2 v2)
    {
        if (0 == v2)
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIVISION_BY_ZERO);
        return v1 % v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryAnd
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 && v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryOr
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 || v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryLess
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 < v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryLessOrEq
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 <= v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryEq
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 == v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryNotEq
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 != v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryGreater
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 > v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryGreaterOrEq
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 >= v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFunctionCall
{
    typedef TR (*FunctionPointer)(T1, T2);
    template<FunctionPointer F>
    struct Function
    {
        template<typename T1_, typename T2_, typename TR1_>
        struct Op
        {
            static TR1_ func(T1_ v1, T2_ v2)
            {
                return F(v1, v2);
            }
        };
    };
};


template<typename T1, typename T2, typename TR>
struct BinaryStringPlus
{
    static VarValue func(VarValue v1, VarValue v2)
    {
        const std::string str1(v1.value->getString());
        const std::string str2(v2.value->getString());
        VarValue res;
        res.value = boost::shared_ptr<Value>(new Value());
        res.value->setString((str1 + str2).c_str());
        return res;
    }
};

template<typename T>
void setVarPart(T& v, const boost::shared_ptr<std::vector<char> >& part)
{
    // Nothing for every datatype except VarType which is specified below
}

template<> inline
void setVarPart<VarValue>(VarValue& v, const boost::shared_ptr<std::vector<char> >& part)
{
    v.varPart = part;
}

/**
 * Fast binary operations
 * i1, i2, ir is index of result in bytes.
 * length is number of elements
 */
template<template <typename T1, typename T2, typename TR> class O, typename T1, typename T2, typename TR>
bool fastDenseBinary(size_t length, const char* p1, size_t i1, const char* p2, size_t i2, char* pr, size_t ir)
{
    // in general case it's impossible to apply fast operations
    return false;
}

template<class B>
bool fastDenseBinaryBool(size_t length, const char* p1, size_t i1, const char* p2, size_t i2, char* pr, size_t ir)
{
    const size_t bitSize = sizeof(unsigned int) * 8;
    if ( (i1 % bitSize == 0) && (i2 % bitSize == 0) && (ir % bitSize == 0) && (length % bitSize == 0) )
    {
        const unsigned int* pi1 = reinterpret_cast<const unsigned int*>(p1) + i1 / bitSize;
        const unsigned int* pi2 = reinterpret_cast<const unsigned int*>(p2) + i2 / bitSize;
        unsigned int* pir = reinterpret_cast<unsigned int*>(pr) + ir / bitSize;
        for (size_t i = 0; i < length / bitSize; i++) {
            *pir++ = B::op(*pi1++, *pi2++);
        }
        return true;
    }
    else
    {
        // in this case it's impossible to apply fast operations.
        return false;
    }
}

/** Boolean fast operation templates */
struct FastBinaryAnd
{
    static unsigned int op(unsigned int a1, unsigned int a2) {
        return a1 & a2;
    }
};

template<> inline
bool fastDenseBinary<BinaryAnd, bool, bool, bool>(size_t length, const char* p1, size_t i1, const char* p2, size_t i2, char* pr, size_t ir)
{
    return fastDenseBinaryBool<FastBinaryAnd>(length, p1, i1, p2, i2, pr, ir);
}

struct FastBinaryOr
{
    static unsigned int op(unsigned int a1, unsigned int a2) {
        return a1 | a2;
    }
};

template<> inline
bool fastDenseBinary<BinaryOr, bool, bool, bool>(size_t length, const char* p1, size_t i1, const char* p2, size_t i2, char* pr, size_t ir)
{
    return fastDenseBinaryBool<FastBinaryOr>(length, p1, i1, p2, i2, pr, ir);
}

/**
 * This template is for binary function for types with fixed size.
 * This function cannot preserve RLE structure.
 * Arguments of this function should be extracted using the same empty bitmask.
 * In other words they should be aligned during unpack.
 */
template<template <typename T1, typename T2, typename TR> class O, typename T1, typename T2, typename TR>
void rle_binary_func(const Value** args, Value* result, void*)
{
    const Value& v1 = *args[0];
    const Value& v2 = *args[1];
    Value& res = *result;
    res.getTile()->clear();
    boost::shared_ptr<std::vector<char> > varPart(new std::vector<char>());

    if (v1.getTile()->count() == 0 ||
        v1.getTile()->nSegments() == 0 ||
        v2.getTile()->count() == 0 ||
        v2.getTile()->nSegments() == 0) {

        res.getTile()->flush(0);
        return;
    }

    size_t i1 = 0;
    size_t i2 = 0;
    RLEPayload::Segment ps1 = v1.getTile()->getSegment(i1);
    uint64_t ps1_length = v1.getTile()->getSegment(i1).length();
    RLEPayload::Segment ps2 = v2.getTile()->getSegment(i2);
    uint64_t ps2_length = v2.getTile()->getSegment(i2).length();
    uint64_t chunkSize = 0;

    if (ps1_length == INFINITE_LENGTH) {
        // v1 is constant with infinity length. Align it pPosition to v2
        ps1._pPosition = ps2._pPosition;
    } else {
        if (ps2_length == INFINITE_LENGTH) {
            // v2 is constant with infinity length. Align it pPosition to v1
            ps2._pPosition = ps1._pPosition;
        }
    }

    while (true)
    {
        // At this point ps1 and ps2 should aligned
        // Segment with less length will be iterated and with more length cut at the end of loop
        assert(ps1._pPosition == ps2._pPosition);

        const uint64_t length = std::min(ps1_length, ps2_length);
        if (length == 0) {
            break;
        }
        RLEPayload::Segment rs;

        // Check NULL cases
        if ( (ps1._same && ps1._null) || (ps2._same && ps2._null) ) {
            rs._same = true;
            rs._null = true;
            rs._pPosition = ps1._pPosition;
            rs._valueIndex = 0; // we do not know what missing reason to use. That's why we set it to 0.
        } else { // No one is NULL and we can evaluate
            rs._null = false;
            rs._pPosition = ps1._pPosition;
            if (ps1._same) {
                if (ps2._same) {
                    rs._same = true;
                    rs._valueIndex = addPayloadValues<TR>(res.getTile());
                    TR r = O<T1, T2, TR>::func(getPayloadValue<T1>(v1.getTile(), ps1._valueIndex),
                                               getPayloadValue<T2>(v2.getTile(), ps2._valueIndex));
                    setVarPart<TR>(r, varPart);
                    setPayloadValue<TR>(res.getTile(), rs._valueIndex, r);
                } else {
                    rs._same = false;
                    rs._valueIndex = addPayloadValues<TR>(res.getTile(), length);
                    size_t i = rs._valueIndex;
                    size_t j = ps2._valueIndex;
                    const size_t end = j + length;
                    while (j < end) {
                        TR r = O<T1, T2, TR>::func(getPayloadValue<T1>(v1.getTile(), ps1._valueIndex),
                                                   getPayloadValue<T2>(v2.getTile(), j++));
                        setVarPart<TR>(r, varPart);
                        setPayloadValue<TR>(res.getTile(), i++, r);
                    }
                }
            } else { // dense non-nullable data
                if (ps2._same) {
                    rs._same = false;
                    rs._valueIndex = addPayloadValues<TR>(res.getTile(), length);
                    size_t i = rs._valueIndex;
                    size_t j = ps1._valueIndex;
                    const size_t end = j + length;
                    while (j < end) {
                        TR r = O<T1, T2, TR>::func(getPayloadValue<T1>(v1.getTile(), j++),
                                                   getPayloadValue<T2>(v2.getTile(), ps2._valueIndex));
                        setVarPart<TR>(r, varPart);
                        setPayloadValue<TR>(res.getTile(), i++, r);
                    }
                } else {
                    rs._same = false;
                    rs._valueIndex = addPayloadValues<TR>(res.getTile(), length);
                    size_t i = rs._valueIndex;
                    size_t j1 = ps1._valueIndex;
                    size_t j2 = ps2._valueIndex;
                    if (!fastDenseBinary<O, T1, T2, TR>(length,
                                                        (const char*)v1.getTile()->getFixData(),
                                                        j1,
                                                        (const char*)v2.getTile()->getFixData(),
                                                        j2,
                                                        res.getTile()->getFixData(),
                                                        int(rs._valueIndex)))
                    {
                        const size_t end = j1 + length;
                        while (j1 < end) {
                            TR r = O<T1, T2, TR>::func(getPayloadValue<T1>(v1.getTile(), j1++),
                                                       getPayloadValue<T2>(v2.getTile(), j2++));
                            setVarPart<TR>(r, varPart);
                            setPayloadValue<TR>(res.getTile(), i++, r);
                        }
                    }
                }
            }
        }
        res.getTile()->addSegment(rs);
        chunkSize = rs._pPosition + length;

        // Moving to the next segments
        if (ps1_length == ps2_length) {
            if (++i1 >= v1.getTile()->nSegments())
                break;
            if (++i2 >= v2.getTile()->nSegments())
                break;
            ps1 = v1.getTile()->getSegment(i1);
            ps1_length = v1.getTile()->getSegment(i1).length();
            ps2 = v2.getTile()->getSegment(i2);
            ps2_length = v2.getTile()->getSegment(i2).length();
        } else {
            if (ps1_length < ps2_length) {
                if (++i1 >= v1.getTile()->nSegments())
                    break;
                ps1 = v1.getTile()->getSegment(i1);
                ps1_length = v1.getTile()->getSegment(i1).length();
                ps2._pPosition += length;
                ps2_length -= length;
                if (!ps2._same)
                    ps2._valueIndex += length;
            } else {
                if (++i2 >= v2.getTile()->nSegments())
                    break;
                ps2 = v2.getTile()->getSegment(i2);
                ps2_length = v2.getTile()->getSegment(i2).length();
                ps1._pPosition += length;
                ps1_length -= length;
                if (!ps1._same)
                    ps1._valueIndex += length;
            }
        }
    }
    res.getTile()->flush(chunkSize);
    if (varPart->size()) {
        res.getTile()->setVarPart(*varPart);
    }
}

void inferIsNullArgTypes(const ArgTypes& factInputArgs, std::vector<ArgTypes>& possibleInputArgs, std::vector<TypeId>& possibleResultArgs);
void rle_unary_bool_is_null(const Value** args, Value* result, void*);
void rle_unary_null_to_any(const Value** args, Value* result, void*);

/**
 * Aggregator classes
 */
template <typename TS, typename TSR>
class AggSum
{
public:
    struct State {
        TSR _sum;
    };

    static void init(State& state)
    {
        state._sum = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._sum += static_cast<TSR>(value);
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._sum += static_cast<TSR>(value) * count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._sum += new_state._sum;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._sum;
        return true;
    }

    static bool final(Value::reason, TSR& result)
    {
        result = 0;
        return true;
    }
};

template <typename TS, typename TSR>
class AggProd
{
public:
    struct State {
        TSR _prod;
    };

    static void init(State& state)
    {
        state._prod = 1;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._prod *= static_cast<TSR>(value);
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        if (count==0)
        {
            return;
        }
        state._prod *= static_cast<TSR>( pow(static_cast<double>(value), static_cast<double>(count)) );
    }

    static void merge(State& state, const State& newState)
    {
        state._prod *= newState._prod;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._prod;
        return true;
    }

    static bool final(Value::reason, TSR& result)
    {
        result = 0;
        return true;
    }
};

template <typename TS, typename TSR>
class AggCount
{
public:
    struct State {
        uint64_t _count;
    };

    static void init(State& state)
    {
        state._count = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._count++;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._count += count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._count += new_state._count;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._count;
        return true;
    }

    static bool final(Value::reason, TSR& result)
    {
        result = 0;
        return true;
    }
};

template<typename T> inline
bool isNanValue(T value)
{
    return false;
}

template<> inline
bool isNanValue<double>(double value)
{
    return isnan(value);
}

template<> inline
bool isNanValue<float>(float value)
{
    return isnan(value);
}

template <typename TS, typename TSR>
class AggMin
{
public:
    struct State {
        TSR _min;
    };

    static void init(State& state, const TS& firstValue)
    {
        state._min = firstValue;
    }

    static void aggregate(State& state, const TS& value)
    {
        if (value < state._min || isNanValue(value))
            state._min = value;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        if (value < state._min || isNanValue(value))
            state._min = value;
    }

    static void merge(State& state, const State& new_state)
    {
        if (new_state._min < state._min || isNanValue(new_state._min))
            state._min = new_state._min;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._min;
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

template <typename TS, typename TSR>
class AggMax
{
public:
    struct State {
        TSR _max;
    };

    static void init(State& state, const TS& firstValue)
    {
        state._max = firstValue;
    }

    static void aggregate(State& state, const TS& value)
    {
        if (value > state._max || isNanValue(value))
            state._max = value;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        if (value > state._max || isNanValue(value))
            state._max = value;
    }

    static void merge(State& state, const State& new_state)
    {
        if (new_state._max > state._max || isNanValue(new_state._max))
            state._max = new_state._max;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._max;
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

template <typename TS, typename TSR>
class AggAvg
{
public:
    struct State {
        TSR _sum;
        uint64_t _count;
    };

    static void init(State& state)
    {
        state._sum = TSR();
        state._count = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._sum += static_cast<TSR>(value);
        state._count++;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._sum += static_cast<TSR>(value) * count;
        state._count += count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._sum += new_state._sum;
        state._count += new_state._count;
    }

    static bool final(const State& state, TSR& result)
    {
        if (state._count == 0)
            return false;
        result = static_cast<TSR>(state._sum) / state._count;
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

template <typename TS, typename TSR>
class AggVar
{
public:
    struct State {
        TSR _m;
        TSR _m2;
        uint64_t _count;
    };

    static void init(State& state)
    {
        state._m = TSR();
        state._m2 = TSR();
        state._count = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._m += static_cast<TSR>(value);
        state._m2 += static_cast<TSR>(value * value);
        state._count++;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._m += static_cast<TSR>(value) * count;
        state._m2 += static_cast<TSR>(value * value) * count;
        state._count += count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._m += new_state._m;
        state._m2 += new_state._m2;
        state._count += new_state._count;
    }

    static bool final(const State& state, TSR& result)
    {
        if (state._count <= 1)
            return false;
        const TSR x = state._m / state._count;
        const TSR s = state._m2 / state._count - x * x;
        result = s * state._count / (state._count - 1) ;
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

template <typename TS, typename TSR>
class AggStDev
{
public:
    struct State {
        TSR _m;
        TSR _m2;
        uint64_t _count;
    };

    static void init(State& state)
    {
        state._m = TSR();
        state._m2 = TSR();
        state._count = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._m += static_cast<TSR>(value);
        state._m2 += static_cast<TSR>(value * value);
        state._count++;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._m += static_cast<TSR>(value) * count;
        state._m2 += static_cast<TSR>(value * value) * count;
        state._count += count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._m += new_state._m;
        state._m2 += new_state._m2;
        state._count += new_state._count;
    }

    static bool final(const State& state, TSR& result)
    {
        if (state._count <= 1)
            return false;
        const TSR x = state._m / state._count;
        const TSR s = state._m2 / state._count - x * x;
        result = sqrt(s * state._count / (state._count - 1) );
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

}

#endif // TILEFUNCTIONS_H
