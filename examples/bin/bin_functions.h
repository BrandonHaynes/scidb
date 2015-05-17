#ifndef BIN_FUNCTIONS_H
#define BIN_FUNCTIONS_H

#include <query/Aggregate.h>

template<template <typename TS, typename TSR> class A, typename T, bool asterisk = false>
  class BinAggregate: public BaseAggregate<A, T, int64_t, asterisk>
{
public:
    BinAggregate(const std::string& name, Type const& aggregateType):
BaseAggregate<A, T, int64_t, asterisk>(name, aggregateType, TypeLibrary::getType(TID_INT64))
    {}

   virtual bool isOrderSensitive()
   {
   return true;
   }
};


template <typename TS, typename TSR>
class AggBin
{
 public:
  struct State {
    TSR _sum;
    uint64_t _count;
  };

  static void init(State& state)
  {
    state._sum = TSR();
    state._count = 10;
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

#endif // BIN_FUNCTIONS_H
