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
 * @file Aggregate.h
 *
 * @author poliocough@gmail.com
 *
 * @brief Aggregate, Aggregate Factory and Aggregate Library headers
 */


#ifndef __AGGREGATE_H__
#define __AGGREGATE_H__

#include <map>

#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "array/RLE.h"
#include "query/TileFunctions.h"
#include "util/arena/Vector.h"

namespace scidb
{

typedef boost::shared_ptr<class Aggregate> AggregatePtr;

/**
 * Base class of aggregate functions.
 *
 * We assume all the aggregates can be computed in a distributed manner.
 * That is, the caller may divide the source data into groups, call the aggregate function in each group, and
 * call the aggregation function over the aggregate results.
 * To support algebraic and holistic aggregate functions, we keep intermediate state.
 * For instance, the state of the algebraic avg() is a running sum and a running count.
 * As another example, the state of the holistic median() is *all* the values.
 *
 * We classify our aggregate functions into two categories: those that are order sensitive, and those aren't.
 * Order-sensitive aggregates, such as last_value(), requires the aggregate functions to be called in a deterministic order.
 * If the AFL operator (e.g. redimension()) cannot guarantee to call the aggregate function in order, we error out.
 * The error will be thrown in the inferSchema() function of some child class of LogicalOperator.
 *
 * Note that an order-sensitive requirement may be satisfied even if the operator is distributed, as long as three conditions are met:
 * (a) Each group only contains consecutive values.
 *     E.g. [1, 2, 3, 4, 5] may be divided into [1, 2] and [3, 4, 5], but not [1, 5] and [2, 3, 4].
 * (b) Within each group, aggregation is applied in order.
 * (c) The intermediate results are aggregated also in order.
 *     E.g. the last_value of the two groups above are 2 and 5, and to get the overall result, last_value needs to see 2 before 5.
 */
class Aggregate
{
public:
    /**
     * Whether aggregation must be applied in a deterministic order.
     * Default is false. Right now, only first_value and last_value (in the enterprise edition) are order sensitive.
     */
    virtual bool isOrderSensitive()
    {
        return false;
    }

protected:
    std::string _aggregateName;
    Type _inputType;
    Type _resultType;

    Aggregate( std::string const& aggregateName,
               Type const& inputType,
               Type const& resultType):
        _aggregateName(aggregateName),
        _inputType(inputType),
        _resultType(resultType)
    {}

public:
    virtual ~Aggregate() {}

    virtual AggregatePtr clone() const        = 0;
    virtual AggregatePtr clone(Type const& aggregateType) const        = 0;

    const std::string& getName() const
    {
        return _aggregateName;
    }

    const Type& getAggregateType() const
    {
        return _inputType;
    }

    virtual Type getStateType() const = 0;

    const Type& getResultType() const
    {
        return _resultType;
    }

    virtual bool supportAsterisk() const { return false; }

    /**
     * This is supposed to be removed.
     */
    virtual bool ignoreZeroes() const
    {
        return false;
    }

    virtual bool ignoreNulls() const
    {
        return false;
    }

    virtual bool isCounting() const
    {
        return false;
    }

    virtual void initializeState(Value& state) = 0;

    /**
     * Accumulate an input value to a state.
     */
    virtual void accumulate(Value& state, Value const& input) = 0;

    /**
     * Accumulate all input values to a state.
     */
    virtual void accumulate(Value& state, std::vector<Value> const& input)
    {
        for (size_t i = 0; i< input.size(); i++)
        {
            accumulate(state, input[i]);
        }
    }

    /**
     * Whether a value qualifies to be accumulated.
     * @note The method does NOT check ignoreZeroes(), because that is supposed to be dead code and should be removed.
     */
    virtual bool qualifyAccumulate(Value const& input) {
        return !(ignoreNulls() && input.isNull());
    }

    /**
     * Call accumulate on a single value, if qualify.
     */
    virtual void tryAccumulate(Value& state, Value const& input) {
        if (qualifyAccumulate(input)) {
            accumulate(state, input);
        }
    }

    /**
     * Call accumulate on multiple values, if qualify.
     */
    virtual void tryAccumulate(Value& state, std::vector<Value> const& input)
    {
        for (size_t i = 0; i< input.size(); i++)
        {
            tryAccumulate(state, input[i]);
        }
    }

    virtual void accumulatePayload(Value& state, ConstRLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();
        bool noNulls = ignoreNulls();

        Value val;
        while (!iter.end())
        {
            if (iter.isNull() == false || noNulls == false)
            {
                iter.getItem(val);
                accumulate(state, val);
                ++iter;
            }
            else
            {
                iter.toNextSegment();
            }
        }
    }

    virtual void merge(Value& dstState, Value const& srcState)  = 0;
    virtual void finalResult(Value& result, Value const& state) = 0;
};

template<template <typename TS, typename TSR> class A, typename T, typename TR, bool asterisk = false>
class BaseAggregate: public Aggregate
{
public:
    BaseAggregate(const std::string& name, Type const& aggregateType, Type const& resultType): Aggregate(name, aggregateType, resultType)
    {}

    AggregatePtr clone() const
    {
        return AggregatePtr(new BaseAggregate(getName(), getAggregateType(), getResultType()));
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return AggregatePtr(new BaseAggregate(getName(), aggregateType, _resultType.typeId() == TID_VOID ? aggregateType : _resultType));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    Type getStateType() const
    {
        Type stateType(TID_BINARY, sizeof(typename A<T, TR>::State) << 3);
        return stateType;
    }

    bool supportAsterisk() const
    {
        return asterisk;
    }

    void initializeState(Value& state)
    {
        state.setVector(sizeof(typename A<T, TR>::State));
        A<T, TR>::init(*static_cast<typename A<T, TR>::State* >(state.data()));
        state.setNull(-1);
    }

    void accumulate(Value& state, Value const& input)
    {
        T value = *reinterpret_cast<T*>(input.data());
        A<T, TR>::aggregate(*static_cast< typename A<T, TR>::State* >(state.data()), value);
    }

    virtual void accumulatePayload(Value& state, ConstRLEPayload const* tile)
    {
        typename A<T, TR>::State& s = *static_cast< typename A<T, TR>::State* >(state.data());
        for (size_t i = 0; i < tile->nSegments(); i++)
        {
            const RLEPayload::Segment& v = tile->getSegment(i);
            if (v._null)
                continue;
            if (v._same) {
                T value = getPayloadValue<T>(tile, v._valueIndex);
                A<T, TR>::multAggregate(s, value, v.length());
            } else {
                const size_t end = v._valueIndex + v.length();
                for (size_t j = v._valueIndex; j < end; j++) {
                    T value = getPayloadValue<T>(tile, j);
                    A<T, TR>::aggregate(s, value);
                }
            }
        }
    }

    void merge(Value& dstState, Value const& srcState)
    {
        A<T, TR>::merge(*static_cast< typename A<T, TR>::State* >(dstState.data()), *static_cast< typename A<T, TR>::State* >(srcState.data()));
    }

    void finalResult(Value& result, Value const& state)
    {
        result.setVector(sizeof(TR));
        if (!A<T, TR>::final(*static_cast< typename A<T, TR>::State* >(state.data()), state.isNull(), *static_cast< TR* >(result.data()))) {
            result.setNull();
        } else {
            result.setNull(-1);
        }
    }
};

template<template <typename TS, typename TSR> class A, typename T, typename TR, bool asterisk = false>
class BaseAggregateInitByFirst: public Aggregate
{
public:
    BaseAggregateInitByFirst(const std::string& name, Type const& aggregateType, Type const& resultType): Aggregate(name, aggregateType, resultType)
    {}

    AggregatePtr clone() const
    {
        return AggregatePtr(new BaseAggregateInitByFirst(getName(), getAggregateType(), getResultType()));
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return AggregatePtr(new BaseAggregateInitByFirst(getName(), aggregateType, _resultType.typeId() == TID_VOID ? aggregateType : _resultType));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    Type getStateType() const
    {
        Type stateType(TID_BINARY, sizeof(typename A<T, TR>::State));
        return stateType;
    }

    bool supportAsterisk() const
    {
        return asterisk;
    }

    void initializeState(Value& state)
    {
        //Here we use missing code 1 for special meaning. It means there have been values
        //accumulated but no valid state yet. This is used by aggregates min() and max() so
        //that min(null, null) returns null. We can't use missing code 0 because that's
        //reserved by the system for groups that do not exist.
        state.setNull(1);
    }

    void accumulate(Value& state, Value const& input)
    {
        //ignoreNulls is true so null input is not allowed
        if (state.isNull())
        {
            T value = *reinterpret_cast<T*>(input.data());
            state.setVector(sizeof(typename A<T, TR>::State));
            A<T, TR>::init(*static_cast<typename A<T, TR>::State* >(state.data()), value);
            state.setNull(-1);
        }
        A<T, TR>::aggregate(*static_cast< typename A<T, TR>::State* >(state.data()),
                            *reinterpret_cast<T*>(input.data()));
    }

    virtual void accumulatePayload(Value& state, ConstRLEPayload const* tile)
    {
        if (!tile->payloadSize()) {
            return;
        }

        if (state.isNull())
        {
            for (size_t i = 0; i < tile->payloadCount(); i++) {
                T value = getPayloadValue<T>(tile, i);
                state.setVector(sizeof(typename A<T, TR>::State));
                typename A<T, TR>::State& si = *static_cast< typename A<T, TR>::State* >(state.data());
                A<T, TR>::init(si, value);
                state.setNull(-1);
                break;
            }
        }
        if (state.isNull()) {
            return;
        }

        typename A<T, TR>::State& s = *static_cast< typename A<T, TR>::State* >(state.data());
        for (size_t i = 0; i < tile->nSegments(); i++)
        {
            const RLEPayload::Segment& v = tile->getSegment(i);
            if (v._null)
                continue;
            if (v._same) {
                A<T, TR>::multAggregate(s, getPayloadValue<T>(tile, v._valueIndex), v.length());
            } else {
                const size_t end = v._valueIndex + v.length();
                for (size_t j = v._valueIndex; j < end; j++) {
                    A<T, TR>::aggregate(s, getPayloadValue<T>(tile, j));
                }
            }
        }
    }

    void merge(Value& dstState, Value const& srcState)
    {
        if(srcState.isNull()) {
            return;
        }
        if(dstState.isNull()) {
            dstState = srcState;
            return;
        }
        A<T, TR>::merge(*static_cast< typename A<T, TR>::State* >(dstState.data()), *static_cast< typename A<T, TR>::State* >(srcState.data()));
    }

    void finalResult(Value& result, Value const& state)
    {
        result.setVector(sizeof(TR));
        if (!A<T, TR>::final(*static_cast< typename A<T, TR>::State* >(state.data()), state.isNull(), *static_cast< TR* >(result.data()))) {
            result.setNull();
        } else {
            result.setNull(-1);
        }
    }
};

class CountingAggregate : public Aggregate
{
protected:
    CountingAggregate(std::string const& aggregateName,
               Type const& inputType,
               Type const& resultType):
        Aggregate(aggregateName, inputType, resultType)
    {}

public:
    virtual bool isCounting() const
    {
        return true;
    }

    virtual bool needsAccumulate() const
    {
        return true;
    }

    virtual void overrideCount(Value& state, uint64_t newCount)   = 0;
};

class AggregateLibrary: public Singleton<AggregateLibrary>
{
private:
    // Map of aggregate factories.
    // '*' for aggregate type means universal aggregate operator which operates by expressions (slow universal implementation).
    typedef std::map < std::string, std::map<TypeId, AggregatePtr>, __lesscasecmp > FactoriesMap;
    FactoriesMap _registeredFactories;

public:
    AggregateLibrary();

    virtual ~AggregateLibrary()
    {}

    void addAggregate(AggregatePtr const& aggregate);

    void getAggregateNames(std::vector<std::string>& names) const;

    size_t getNumAggregates() const
    {
        return _registeredFactories.size();
    }

    bool hasAggregate(std::string const& aggregateName) const
    {
        return _registeredFactories.find(aggregateName) != _registeredFactories.end();
    }

    AggregatePtr createAggregate(std::string const& aggregateName, Type const& aggregateType) const;
};


/**
 * An @c AggIOMapping associates an input attribute with N output
 * attributes and their corresponding @c Aggregate objects.
 */
class AggIOMapping
{
public:
    AggIOMapping()
        : _inputAttributeId(INVALID_ATTRIBUTE_ID)
    {}

    AggIOMapping(AttributeID inAttId, AttributeID outAttId, AggregatePtr agg)
        : _inputAttributeId(inAttId)
        , _outputAttributeIds(1, outAttId)
        , _aggregates(1, agg)
    {}

    void setInputAttributeId(AttributeID id) { _inputAttributeId = id; }
    int64_t getInputAttributeId() const { return _inputAttributeId; }
    bool validAttributeId() const { return _inputAttributeId != INVALID_ATTRIBUTE_ID; }

    AggregatePtr getAggregate(size_t i) const { return _aggregates[i]; }
    AttributeID getOutputAttributeId(size_t i) const { return _outputAttributeIds[i]; }

    // Not a good idea to give out handles to private data, but saves
    // N smart pointer copies in PhysicalVariableWindow.  *Sigh*, OK, OK.
    const std::vector<AggregatePtr>& getAggregates() const { return _aggregates; }

    size_t size() const { return _aggregates.size(); }
    bool empty() const { return _aggregates.empty(); }

    void push_back(AttributeID id, AggregatePtr ptr)
    {
        _outputAttributeIds.push_back(id);
        _aggregates.push_back(ptr);
    }

    void merge(const AggIOMapping& other)
    {
        assert(other._outputAttributeIds.size() == other._aggregates.size());
        _outputAttributeIds.insert(_outputAttributeIds.end(),
                                   other._outputAttributeIds.begin(),
                                   other._outputAttributeIds.end());
        _aggregates.insert(_aggregates.end(),
                           other._aggregates.begin(),
                           other._aggregates.end());
    }

private:
    AttributeID _inputAttributeId;

    // Parallel arrays, _outAttr[i] corresponds to _agg[i].
    std::vector<AttributeID> _outputAttributeIds;
    std::vector<AggregatePtr> _aggregates;
};

} //namespace scidb

#endif
