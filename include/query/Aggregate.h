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

#ifndef AGGREGATE_H_
#define AGGREGATE_H_

#include <map>

#include <query/TypeSystem.h>
#include <array/Metadata.h>
#include <array/RLE.h>
#include <array/StreamArray.h>
#include "query/TileFunctions.h"
#include "util/Singleton.h"

namespace scidb
{
class Query;
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

    /*
     * Note from Donghui Zhang 10/8/2014:
     * I re-engineered from the code that missingReason==0 was used to represent the case that a state has not been initialized.
     * Without changing the behavior of the system, I replaced all evaluations  getMissingReason()==0 with this function.
     * The goal is to keep in a single place such logic, should we decide to change the implementation of the condition.
     * Also, such refactoring makes the code easier to understand, I believe.
     *
     * @param state  a state.
     * @return whether the state has been initialized.
     */
    bool isStateInitialized(Value const& state) const
    {
        return state.getMissingReason() != 0;
    }

    /**
     * Whether a state qualifies to be merged.
     * @param srcState  a source state.
     * @return whether the state qualifies to be merged from.
     * @note: Normally, a state can be merged as long as it is initialized.
     *        But derived classes may override this.
     *        E.g. in BaseAggregateInitByFirst, if missingReason==1 means, even though the state is initialized, it is not ready to be merged.
     */
    virtual bool isMergeable(Value const& srcState) const
    {
        return isStateInitialized(srcState);
    }

    /**
     * Whether a value qualifies to be accumulated.
     * @param srcValue  a source value.
     * @return whether the value qualifies to be accumulated.
     */
    virtual bool isAccumulatable(Value const& srcValue) const
    {
        return !(ignoreNulls() && srcValue.isNull());
    }

    /**
     * Accumulate an input value to a state.
     * @param dstState     a destination state, which MUST have be initialized.
     * @param srcValue     a source value, which MUST have isAccumulatable()==true.
     */
    virtual void accumulate(Value& state, Value const& input) = 0;

    /**
     * Merge a state into another state.
     * @param dstState  the destination state, which MUST have been initialized.
     * @param srcState  the source state, which MUST have isMergeable()==true.
     *
     */
    virtual void merge(Value& dstState, Value const& srcState)  = 0;

public:
    virtual ~Aggregate() {}

    virtual AggregatePtr clone()                          const = 0;
    virtual AggregatePtr clone(Type const& aggregateType) const = 0;

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

    virtual bool supportAsterisk() const
    {
        return false;
    }

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
     * Initialize the state if not already, then call accumulate on a single value, if is is ready to be accumulated from.
     * @param dstState   destination state.
     * @param srcValue   a source value.
     */
    virtual void accumulateIfNeeded(Value& dstState, Value const& srcValue) {
        if (! isStateInitialized(dstState)) {
            initializeState(dstState);
            assert(isStateInitialized(dstState));
        }

        if (isAccumulatable(srcValue)) {
            accumulate(dstState, srcValue);
        }
    }

    /**
     * Initialize the state if not already, then accumulate a payload of values.
     * @param dstState   a destination state.
     * @param srcValues  a vector of source values.
     */
    virtual void accumulateIfNeeded(Value& dstState, ConstRLEPayload const* tile)
    {
        if (! isStateInitialized(dstState)) {
            initializeState(dstState);
            assert(isStateInitialized(dstState));
        }

        ConstRLEPayload::iterator iter = tile->getIterator();
        bool noNulls = ignoreNulls();

        Value val;
        while (!iter.end())
        {
            if (iter.isNull() == false || noNulls == false)
            {
                iter.getItem(val);
                accumulate(dstState, val);
                ++iter;
            }
            else
            {
                iter.toNextSegment();
            }
        }
    }

    /**
     * Initialize the state if not already, then merge a source state into a destination state, if the source state is ready to merge from.
     * @param dstState  the destination state, which MUST have been initialized.
     * @param srcState  the source state.
     *
     */
    virtual void mergeIfNeeded(Value& dstState, Value const& srcState)
    {
        if (! isStateInitialized(dstState)) {
            initializeState(dstState);
            assert(isStateInitialized(dstState));
        }

        if (isMergeable(srcState)) {
            merge(dstState, srcState);
        }
    }

    /**
     * Turn the intermediate aggregation state into a value.
     * @param dstValue  placeholder for the destination value.
     * @param srcState  the aggregation state.
     * @note srcState may or may not have been initialized.
     */
    virtual void finalResult(Value& dstValue, Value const& srcState) = 0;
};

template<template <typename TS, typename TSR> class A, typename T, typename TR, bool asterisk = false>
class BaseAggregate: public Aggregate
{
protected:
    typedef          A<T,TR>        Agg;
    typedef typename A<T,TR>::State State;

protected:
    void accumulate(Value& dstState, Value const& srcValue)
    {
        assert(isStateInitialized(dstState));
        assert(isAccumulatable(srcValue));

        Agg::aggregate(dstState.get<State>(), srcValue.get<T>());
    }

    void merge(Value& dstState, Value const& srcState)
    {
        assert(isStateInitialized(dstState));
        assert(isMergeable(srcState));

        Agg::merge(dstState.get<State>(),srcState.get<State>());
    }

public:
    BaseAggregate(const std::string& name, Type const& aggregateType, Type const& resultType): Aggregate(name, aggregateType, resultType)
    {}

    AggregatePtr clone() const
    {
        return boost::make_shared<BaseAggregate>(getName(), getAggregateType(), getResultType());
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return boost::make_shared<BaseAggregate>(getName(), aggregateType, _resultType.typeId() == TID_VOID ? aggregateType : _resultType);
    }

    bool ignoreNulls() const
    {
        return true;
    }

    Type getStateType() const
    {
        return Type(TID_BINARY, sizeof(State) * CHAR_BIT);
    }

    bool supportAsterisk() const
    {
        return asterisk;
    }

    void initializeState(Value& state)
    {
        state.setSize(sizeof(State));
        Agg::init(state.get<State>());
    }

    virtual void accumulateIfNeeded(Value& state, ConstRLEPayload const* tile)
    {
        if (! isStateInitialized(state)) {
            initializeState(state);
            assert(isStateInitialized(state));
        }

        State& s = state.get<State>();

        for (size_t i=0,n=tile->nSegments(); i < n; i++)
        {
            const RLEPayload::Segment& v = tile->getSegment(i);
            if (v._null)
                continue;
            if (v._same) {
                Agg::multAggregate(s, getPayloadValue<T>(tile, v._valueIndex), v.length());
            } else {
                const size_t end = v._valueIndex + v.length();
                for (size_t j = v._valueIndex; j < end; j++) {
                    Agg::aggregate(s, getPayloadValue<T>(tile, j));
                }
            }
        }
    }

    void finalResult(Value& dstValue, Value const& srcState)
    {
        dstValue.setSize(sizeof(TR));
        bool valid;

        if (srcState.isNull())
        {
            valid = Agg::final(srcState.getMissingReason(),dstValue.get<TR>());
        }
        else
        {
            valid = Agg::final(srcState.get<State>(),dstValue.get<TR>());
        }

        if (!valid)
        {
            dstValue.setNull();
        }
    }
};

/**
 * In this class, missingReason==1 means state is initialized but not ready to merge from.
 * Also, inherited from Aggregate: missingReason==0 means state is not initialized.
 */
template<template <typename TS, typename TSR> class A, typename T, typename TR, bool asterisk = false>
class BaseAggregateInitByFirst: public Aggregate
{
protected:
    typedef          A<T,TR>        Agg;
    typedef typename A<T,TR>::State State;

protected:
    void accumulate(Value& dstState, Value const& srcValue)
    {
        assert(isStateInitialized(dstState) );
        assert( isAccumulatable(srcValue) );

        if ( !isMergeable(dstState) )
        {
            dstState.setSize(sizeof(State));
            Agg::init(dstState.get<State>(), srcValue.get<T>());
        }
        Agg::aggregate(dstState.get<State>(),srcValue.get<T>());
    }

    void merge(Value& dstState, Value const& srcState)
    {
        assert(isStateInitialized(dstState));
        assert(isMergeable(srcState));

        if ( !isMergeable(dstState) ) {
            dstState = srcState;
            return;
        }
        Agg::merge(dstState.get<State>(), srcState.get<State>());
    }

    virtual bool isMergeable(Value const& srcState) const
    {
        if (! isStateInitialized(srcState)) {
            return false;
        }
        if (srcState.getMissingReason()==1) {
            return false;
        }
        assert(!srcState.isNull());
        return true;
    }

public:
    BaseAggregateInitByFirst(const std::string& name, Type const& aggregateType, Type const& resultType): Aggregate(name, aggregateType, resultType)
    {}

    AggregatePtr clone() const
    {
        return boost::make_shared<BaseAggregateInitByFirst>(getName(), getAggregateType(), getResultType());
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return boost::make_shared<BaseAggregateInitByFirst>(getName(), aggregateType, _resultType.typeId() == TID_VOID ? aggregateType : _resultType);
    }

    bool ignoreNulls() const
    {
        return true;
    }

    Type getStateType() const
    {
        return Type(TID_BINARY, sizeof(State) * CHAR_BIT);
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

    virtual void accumulateIfNeeded(Value& state, ConstRLEPayload const* tile)
    {
        if (! isStateInitialized(state)) {
            initializeState(state);
            assert(isStateInitialized(state));
        }

        if (!tile->payloadSize()) {
            return;
        }

        if (! isMergeable(state))
        {
            for (size_t i = 0; i < tile->payloadCount(); i++) {
                state.setSize(sizeof(State));
                Agg::init(state.get<State>(), getPayloadValue<T>(tile, i));
                break;
            }
        }
        if (!isMergeable(state)) {
            return;
        }
        assert(! state.isNull());

        State& s = state.get<State>();

        for (size_t i=0,n=tile->nSegments(); i < n; i++)
        {
            const RLEPayload::Segment& v = tile->getSegment(i);
            if (v._null)
                continue;
            if (v._same) {
                Agg::multAggregate(s, getPayloadValue<T>(tile, v._valueIndex), v.length());
            } else {
                const size_t end = v._valueIndex + v.length();
                for (size_t j = v._valueIndex; j < end; j++) {
                    Agg::aggregate(s, getPayloadValue<T>(tile, j));
                }
            }
        }
    }

    void finalResult(Value& dstValue, Value const& srcState)
    {
        dstValue.setSize(sizeof(TR));
        bool valid;

        if (srcState.isNull())
        {
            valid = Agg::final(srcState.getMissingReason(),dstValue.get<TR>());
        }
        else
        {
            valid = Agg::final(srcState.get<State>(),dstValue.get<TR>());
        }

        if (!valid)
        {
            dstValue.setNull();
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

    size_t size()  const { return _aggregates.size(); }
    bool   empty() const { return _aggregates.empty(); }

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

#ifndef SCIDB_CLIENT
/**
 * A partial chunk merger which uses an aggregate function to form the complete chunk.
 * It expects the partial chunks to contain aggreagte state value suitable for using with the Aggregate methods.
 */
class AggregateChunkMerger : public MultiStreamArray::PartialChunkMerger
{
protected:
    AggregatePtr const _aggregate;
private:
    bool const _isEmptyable;
    boost::shared_ptr<MemChunk> _mergedChunk;

public:
    /// Constructor
    AggregateChunkMerger(AggregatePtr const& agg,
                     bool isEmptyable)
    : _aggregate(agg),
      _isEmptyable(isEmptyable)
    {
        assert(_aggregate);
    }

    /// Destructor
    ~AggregateChunkMerger() {}

    /// Clear the internal state in preparation for the next chunk (position)
    void clear();

    /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunk
    virtual bool mergePartialChunk(InstanceID instanceId,
                                   AttributeID attId,
                                   boost::shared_ptr<MemChunk>& chunk,
                                   const boost::shared_ptr<Query>& query);

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    virtual boost::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                       const boost::shared_ptr<Query>& query);
};
#endif

} //namespace scidb

#endif
