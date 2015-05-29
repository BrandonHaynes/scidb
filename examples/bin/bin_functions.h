#ifndef BIN_FUNCTIONS_H
#define BIN_FUNCTIONS_H

#include <query/Aggregate.h>

template<template <typename TS, typename TSR> class A, typename T, bool asterisk = false>
class BinAggregate: public BaseAggregate<A, T, int64_t, asterisk>
{
 public:
    BinAggregate(const std::string& name, Type const& aggregateType):
        BaseAggregate<A, T, int64_t, asterisk>(name, aggregateType, TypeLibrary::getType(TID_INT64))
    { }
};

template <typename TS, typename TSR, int64_t range, int64_t bins>
class AggBin
{
 public:
  struct State {
    TSR _value;
    bool _hasValue;
  };

  static void init(State& state)
  {
    state._hasValue = false;
    state._value = TSR();
  }

  static void aggregate(State& state, const TS& value)
  {
    state._value = state._hasValue ? static_cast<TSR>(value) - state._value : static_cast<TSR>(value);
    state._hasValue = true;
  }

  static void multAggregate(State& state, const TS& value, uint64_t count)
  {
    aggregate(state, static_cast<TSR>(value));
  }

  static void merge(State& state, const State& new_state)
  {
  }

  static bool final(const State& state, TSR& result)
  {
    int64_t low = -range;
    int64_t high = range;
    result = (int64_t)((double)(bins - 1) * (state._value - low) / (high - low));

    return true;
  }

  static bool final(Value::reason, TSR&)
  {
    return false;
  }
};

#define AGGBIN(range, bins)                                 \
template <typename TS, typename TSR>                        \
  class AggBin##range: public AggBin<TS, TSR, range, bins>  \
{ }                                                         \

class SignedCountAggregate : public CountingAggregate
{
 private:
  bool _ignoreNulls;

 protected:
  void accumulate(Value& dstState, Value const& srcValue)
  {
    assert(isStateInitialized(dstState));
    assert(isAccumulatable(srcValue));

    (*dstState.getData<int64_t>())++;
  }

  void merge(Value& dstState, Value const& srcState)
  {
    assert(isStateInitialized(dstState));
    assert(isMergeable(srcState));

    (*dstState.getData<int64_t>()) += srcState.getInt64();
  }

 public:
 SignedCountAggregate(Type const& aggregateType):
  CountingAggregate("signed_count", aggregateType, TypeLibrary::getType(TID_INT64)), _ignoreNulls(aggregateType.typeId() != TID_VOID)
    {}

  AggregatePtr clone() const
  {
    return AggregatePtr(new SignedCountAggregate(getAggregateType()));
  }

  AggregatePtr clone(Type const& aggregateType) const
  {
    return AggregatePtr(new SignedCountAggregate(aggregateType));
  }

  Type getStateType() const
  {
    return TypeLibrary::getType(TID_INT64);
  }

  bool supportAsterisk() const
  {
    return true;
  }

  void initializeState(Value& state)
  {
    state = Value(getStateType());
    state = TypeLibrary::getDefaultValue(getStateType().typeId());
  }

  bool ignoreNulls() const
  {
    return _ignoreNulls;
  }

  bool needsAccumulate() const
  {
    //if _ignoreNulls is true, that means this is a "count(attribute_name)" aggregate.
    //this can be made more optimal but right now we are disabling the optimization to fix the bug
    //aggregate(build(<v:double null>[i=1:2,2,0],null), count(v)) --> 0
    return _ignoreNulls;
  }

  void accumulateIfNeeded(Value& state, ConstRLEPayload const* tile)
  {
    if (! isStateInitialized(state)) {
      initializeState(state);
      assert(isStateInitialized(state));
    }

    if (!ignoreNulls()) {
      *state.getData<int64_t>() += tile->count();
    } else {
      ConstRLEPayload::iterator iter = tile->getIterator();
      while (!iter.end())
  {
    if (!iter.isNull())
      {
        *state.getData<int64_t>() += iter.getSegLength();
      }
    iter.toNextSegment();
  }
    }
  }

  void overrideCount(Value& state, uint64_t newCount)
  {
    *state.getData<int64_t>() = (int64_t)newCount;
  }

  void finalResult(Value& dstValue, Value const& srcState)
  {
    if (! isMergeable(srcState))
      {
  dstValue = TypeLibrary::getDefaultValue(getResultType().typeId());
      }
    else {
      dstValue = srcState;
    }
  }
};

#endif // BIN_FUNCTIONS_H
