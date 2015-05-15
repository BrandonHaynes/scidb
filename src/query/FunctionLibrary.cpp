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
 * @file FunctionLibrary.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <cmath>
#include <boost/format.hpp>
#include <boost/make_shared.hpp>
#include <boost/regex.hpp>
#include <boost/assign.hpp>

#include "query/TypeSystem.h"
#include "query/FunctionDescription.h"
#include "query/Expression.h"
#include "query/LogicalExpression.h"
#include "system/SystemCatalog.h"
#include "array/DBArray.h"
#include "network/NetworkManager.h"

#include "query/FunctionLibrary.h"
#include "util/PluginManager.h"
#include "query/TileFunctions.h"
#include "query/Aggregate.h"

using namespace std;
using namespace boost;
using namespace boost::assign;

namespace scidb
{

// including implementations of the built-in functions
#include "query/BuiltInFunctions.h"

#define BINARY_OP(LN, T, R, PN, CN, TM, RM) \
    addFunction(FunctionDescription(PN, list_of(TypeId(T))(TypeId(T)), R, &LN##_##T, (size_t)0));

#define BINARY_BOP(LN, T, R, PN, CN, TM, RM) BINARY_OP(LN, T, R, PN, CN, TM, RM)

#define BINARY_BBOP(LN, T, R, PN, CN, TM, RM) BINARY_OP(LN, T, R, PN, CN, TM, RM)

#define LOGICAL_OP(LN, T, R, PN, CN, OP, TM, RM) BINARY_OP(LN, T, R, PN, CN, TM, RM)

#define LOGICAL_AND(LN, T, R, PN) BINARY_OP(LN, T, R, PN, &&, Bool, Bool)

#define LOGICAL_OR(LN, T, R, PN) BINARY_OP(LN, T, R, PN, ||, Bool, Bool)

#define DIVISION_OP(LN, T, R, PN, CN, TM, RM) BINARY_OP(LN, T, R, PN, CN, TM, RM)

#define UNARY_OP(LN, T, R, PN, CN, TM, RM) \
        addFunction(FunctionDescription(PN, list_of(TypeId(T)), R, &UNARY_##LN##_##T, (size_t)0));

#define UNARY_NOT(LN, T, R, PN, CN, TM, RM) UNARY_OP(LN, T, R, PN, CN, TM, RM)

#define FUNCTION_A1(LN, T, R, PN, CN, TM, RM) \
        addFunction(FunctionDescription(PN, list_of(TypeId(T)), R, &LN##_##T, (size_t)0));

#define FUNCTION_A2(LN, T1, T2, R, PN, CN, T1M, T2M, RM) \
        addFunction(FunctionDescription(PN, list_of(TypeId(T1))(TypeId(T2)), R, &LN##_##T1##_##T2, (size_t)0));

#define CONVERTOR(T, R, TM, RM, COST)                                    \
    addConverter(T, R, &CONV##_##T##_##TO##_##R, COST);

#define CONVERTOR_BOOL(T, R, TM, RM, COST) CONVERTOR(T, R, TM, RM, COST)

#define CONVERTOR_TO_STR(T, TM)                                    \
    addConverter(T, TID_STRING, &CONV##_##T##_##TO_String, EXPLICIT_CONVERSION_COST);

#define CONVERTOR_FROM_STR(T, TM)                                  \
    addConverter(TID_STRING, T, &CONV##_##T##_##FROM_String, EXPLICIT_CONVERSION_COST);

#define CONVERTOR_STR_TO_OCTET(T, TM)   CONVERTOR_FROM_STR(T, TM)

// FunctionLibrary implementation
FunctionLibrary::FunctionLibrary(): _registeringBuiltInObjects(false)
{
#ifdef SCIDB_CLIENT
    registerBuiltInFunctions();
#endif
}

template<typename T>
class ValueKeeper
{
private:
    T& _valueRef;
    T _value;
public:
        ValueKeeper(T& value): _valueRef(value), _value(value) {
    }
    ~ValueKeeper() {
        _valueRef = _value;
    }
};

void FunctionLibrary::registerBuiltInFunctions()
{
    ValueKeeper<bool> valueKeeper(_registeringBuiltInObjects);
    _registeringBuiltInObjects = true;
// Adding built-in functions and converters
#include "query/BuiltInFunctions.inc"
    addConverter(TID_CHAR, TypeId(TID_STRING), &convChar2Str, TRANSFORM_CONVERSION_COST);
    addConverter(TypeId(TID_STRING), TID_CHAR, &convStr2Char, TRUNCATE_CONVERSION_COST);
// Here you should register more difficult functions, implemented in BuiltInFunctions.h
#ifndef SCIDB_CLIENT
    addFunction(FunctionDescription("format", list_of(TID_DOUBLE)(TID_STRING), TypeId(TID_STRING), &formatDouble, (size_t)0));
    addFunction(FunctionDescription("length", list_of(TID_STRING)(TID_STRING), TypeId(TID_INT64), &length, (size_t)0));
    addFunction(FunctionDescription("first_index", list_of(TID_STRING)(TID_STRING), TypeId(TID_INT64), &first_index, (size_t)0));
    addFunction(FunctionDescription("last_index", list_of(TID_STRING)(TID_STRING), TypeId(TID_INT64), &last_index, (size_t)0));
    addFunction(FunctionDescription("low", list_of(TID_STRING)(TID_STRING), TypeId(TID_INT64), &low, (size_t)0));
    addFunction(FunctionDescription("high", list_of(TID_STRING)(TID_STRING), TypeId(TID_INT64), &high, (size_t)0));
    addFunction(FunctionDescription("length", list_of(TID_STRING), TypeId(TID_INT64), &length1, (size_t)0));
    addFunction(FunctionDescription("first_index", list_of(TID_STRING), TypeId(TID_INT64), &first_index1, (size_t)0));
    addFunction(FunctionDescription("last_index", list_of(TID_STRING), TypeId(TID_INT64), &last_index1, (size_t)0));
    addFunction(FunctionDescription("low", list_of(TID_STRING), TypeId(TID_INT64), &low1, (size_t)0));
    addFunction(FunctionDescription("high", list_of(TID_STRING), TypeId(TID_INT64), &high1, (size_t)0));
    addFunction(FunctionDescription("instanceid", vector<TypeId>(), TypeId(TID_INT64), &instanceId, (size_t)0));
#endif
    addFunction(FunctionDescription("missing", list_of(TID_INT32), TID_VOID, &missing, 0));
    addFunction(FunctionDescription("is_null", list_of(TID_VOID), TID_VOID, &isNull, 0, false, &inferIsNullArgTypes));
    addFunction(FunctionDescription("is_nan", list_of(TID_DOUBLE), TID_BOOL, &isNan, 0));
    addFunction(FunctionDescription("strchar", list_of(TID_STRING), TID_CHAR, &strchar, (size_t)0));
    addFunction(FunctionDescription("+", list_of(TID_STRING)(TID_STRING), TypeId(TID_STRING), &strPlusStr, (size_t)0));
    addFunction(FunctionDescription("substr", list_of(TID_STRING)(TID_INT32)(TID_INT32), TypeId(TID_STRING), &subStr, (size_t)0));
    addFunction(FunctionDescription("strlen", list_of(TID_STRING), TypeId(TID_INT32), &strLen, (size_t)0));
    addFunction(FunctionDescription("=", list_of(TID_STRING)(TID_STRING), TypeId(TID_BOOL), &strEq, (size_t)0));
    addFunction(FunctionDescription("regex", list_of(TID_STRING)(TID_STRING), TypeId(TID_BOOL), &strRegex, (size_t)0));
    addFunction(FunctionDescription("<>", list_of(TID_STRING)(TID_STRING), TypeId(TID_BOOL), &strNotEq, (size_t)0));
    addFunction(FunctionDescription("<", list_of(TID_STRING)(TID_STRING), TypeId(TID_BOOL), &strLess, (size_t)0));
    addFunction(FunctionDescription(">", list_of(TID_STRING)(TID_STRING), TypeId(TID_BOOL), &strGreater, (size_t)0));
    addFunction(FunctionDescription("min", list_of(TID_STRING)(TID_STRING), TypeId(TID_STRING), &strMin, (size_t)0));
    addFunction(FunctionDescription("max", list_of(TID_STRING)(TID_STRING), TypeId(TID_STRING), &strMax, (size_t)0));
    addFunction(FunctionDescription("min", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &boolMin, (size_t)0));
    addFunction(FunctionDescription("max", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &boolMax, (size_t)0));
    addFunction(FunctionDescription("<=", list_of(TID_STRING)(TID_STRING), TypeId(TID_BOOL), &strLessOrEq, (size_t)0));
    addFunction(FunctionDescription(">=", list_of(TID_STRING)(TID_STRING), TypeId(TID_BOOL), &strGreaterOrEq, (size_t)0));
    addFunction(FunctionDescription("strftime", list_of(TID_DATETIME)(TID_STRING), TypeId(TID_STRING), &strFTime, (size_t)0));
    addFunction(FunctionDescription("now", vector<TypeId>(), TypeId(TID_DATETIME), &currentTime, (size_t)0,
                                    /*commulativity*/ false, /*inferFuncArgTypes*/ NULL, /*isDeterministic*/ false));
    addFunction(FunctionDescription("+", list_of(TID_DATETIME)(TID_INT64), TypeId(TID_DATETIME), &addIntToDateTime, (size_t)0));
    addFunction(FunctionDescription("-", list_of(TID_DATETIME)(TID_INT64), TypeId(TID_DATETIME), &subIntFromDateTime, (size_t)0));
    addFunction(FunctionDescription("random", vector<TypeId>(), TypeId(TID_UINT32), &scidb_random, (size_t)0,
                                    /*commulativity*/ false, /*inferFuncArgTypes*/ NULL, /*isDeterministic*/ false));
    addConverter(TID_DATETIME, TID_STRING, &convDateTime2Str, EXPLICIT_CONVERSION_COST);
    addConverter(TID_STRING, TID_DATETIME, &convStr2DateTime, EXPLICIT_CONVERSION_COST);//TRANSFORM_CONVERSION_COST);

    addFunction(FunctionDescription("togmt", list_of(TID_DATETIMETZ), TypeId(TID_DATETIME), &tzToGmt, (size_t)0));
    addFunction(FunctionDescription("strip_offset", list_of(TID_DATETIMETZ), TypeId(TID_DATETIME), &stripOffset, (size_t)0));
    addFunction(FunctionDescription("append_offset", list_of(TID_DATETIME)(TID_INT64), TypeId(TID_DATETIMETZ), &appendOffset, (size_t)0));
    addFunction(FunctionDescription("apply_offset", list_of(TID_DATETIME)(TID_INT64), TypeId(TID_DATETIMETZ), &applyOffset, (size_t)0));
    addFunction(FunctionDescription("get_offset", list_of(TID_DATETIMETZ), TypeId(TID_INT64), &getOffset, (size_t)0));
    addFunction(FunctionDescription("=", list_of(TID_DATETIMETZ)(TID_DATETIMETZ), TypeId(TID_BOOL), &tzEq, (size_t)0));
    addFunction(FunctionDescription("<>", list_of(TID_DATETIMETZ)(TID_DATETIMETZ), TypeId(TID_BOOL), &tzNotEq, (size_t)0));
    addFunction(FunctionDescription("<", list_of(TID_DATETIMETZ)(TID_DATETIMETZ), TypeId(TID_BOOL), &tzLess, (size_t)0));
    addFunction(FunctionDescription(">", list_of(TID_DATETIMETZ)(TID_DATETIMETZ), TypeId(TID_BOOL), &tzGreater, (size_t)0));
    addFunction(FunctionDescription("<=", list_of(TID_DATETIMETZ)(TID_DATETIMETZ), TypeId(TID_BOOL), &tzLessOrEq, (size_t)0));
    addFunction(FunctionDescription(">=", list_of(TID_DATETIMETZ)(TID_DATETIMETZ), TypeId(TID_BOOL), &tzGreaterOrEq, (size_t)0));
    addFunction(FunctionDescription("tznow", vector<TypeId>(), TypeId(TID_DATETIMETZ), &currentTimeTz, (size_t)0,
                                    /*commulativity*/ false, /*inferFuncArgTypes*/ NULL, /*isDeterministic*/ false));

addFunction(FunctionDescription("day_of_week", list_of(TID_DATETIME), TypeId(TID_UINT8), &dayOfWeekT, (size_t) 0));
    addFunction(FunctionDescription("hour_of_day", list_of(TID_DATETIME), TypeId(TID_UINT8), &hourOfDayT, (size_t) 0));
    addFunction(FunctionDescription("day_of_week", list_of(TID_DATETIMETZ), TypeId(TID_UINT8), &dayOfWeekTZ, (size_t) 0));
    addFunction(FunctionDescription("hour_of_day", list_of(TID_DATETIMETZ), TypeId(TID_UINT8), &hourOfDayTZ, (size_t) 0));

    addConverter(TID_DATETIMETZ, TID_STRING, &convDateTimeTz2Str, EXPLICIT_CONVERSION_COST);
    addConverter(TID_STRING, TID_DATETIMETZ, &convStr2DateTimeTz, EXPLICIT_CONVERSION_COST);



/**
 * Section of new vector functions.
 */
    // TID_CHAR
    addVFunction(FunctionDescription("=", list_of(TID_CHAR)(TID_CHAR), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Char, Char, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_CHAR)(TID_CHAR), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Char, Char, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_CHAR)(TID_CHAR), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Char, Char, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_CHAR)(TID_CHAR), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Char, Char, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_CHAR)(TID_CHAR), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Char, Char, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_CHAR)(TID_CHAR), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Char, Char, Bool>, 0));

    addVConverter(TID_CHAR, TID_INT8, &rle_unary_func<UnaryConverter, Char, Int8>, 1);
    addVConverter(TID_CHAR, TID_INT16, &rle_unary_func<UnaryConverter, Char, Int16>, 2);
    addVConverter(TID_CHAR, TID_INT32, &rle_unary_func<UnaryConverter, Char, Int32>, 3);
    addVConverter(TID_CHAR, TID_INT64, &rle_unary_func<UnaryConverter, Char, Int64>, 4);
    addVConverter(TID_CHAR, TID_UINT8, &rle_unary_func<UnaryConverter, Char, Uint8>, 1);
    addVConverter(TID_CHAR, TID_UINT16, &rle_unary_func<UnaryConverter, Char, Uint16>, 2);
    addVConverter(TID_CHAR, TID_UINT32, &rle_unary_func<UnaryConverter, Char, Uint32>, 3);
    addVConverter(TID_CHAR, TID_UINT64, &rle_unary_func<UnaryConverter, Char, Uint64>, 4);
    addVConverter(TID_CHAR, TID_FLOAT, &rle_unary_func<UnaryConverter, Char, Float>, TRANSFORM_CONVERSION_COST*2);
    addVConverter(TID_CHAR, TID_DOUBLE, &rle_unary_func<UnaryConverter, Char, Double>, TRANSFORM_CONVERSION_COST);
    addVConverter(TID_CHAR, TID_BOOL, &rle_unary_func<UnaryConverter, Char, Bool>, TRANSFORM_CONVERSION_COST);

    // TID_INT8
    addVFunction(FunctionDescription("+", list_of(TID_INT8)(TID_INT8), TypeId(TID_INT8), &rle_binary_func<BinaryPlus, Int8, Int8, Int8>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_INT8)(TID_INT8), TypeId(TID_INT8), &rle_binary_func<BinaryMinus, Int8, Int8, Int8>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_INT8)(TID_INT8), TypeId(TID_INT8), &rle_binary_func<BinaryMult, Int8, Int8, Int8>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_INT8)(TID_INT8), TypeId(TID_INT8), &rle_binary_func<BinaryDiv, Int8, Int8, Int8>, 0));
    addVFunction(FunctionDescription("%", list_of(TID_INT8)(TID_INT8), TypeId(TID_INT8), &rle_binary_func<BinaryMod, Int8, Int8, Int8>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_INT8)(TID_INT8), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Int8, Int8, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_INT8)(TID_INT8), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Int8, Int8, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_INT8)(TID_INT8), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Int8, Int8, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_INT8)(TID_INT8), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Int8, Int8, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_INT8)(TID_INT8), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Int8, Int8, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_INT8)(TID_INT8), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Int8, Int8, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_INT8), TypeId(TID_INT8), &rle_unary_func<UnaryMinus, Int8, Int8>, 0));

    addVConverter(TID_INT8, TID_INT16, &rle_unary_func<UnaryConverter, Int8, Int16>, 2);
    addVConverter(TID_INT8, TID_INT32, &rle_unary_func<UnaryConverter, Int8, Int32>, 3);
    addVConverter(TID_INT8, TID_INT64, &rle_unary_func<UnaryConverter, Int8, Int64>, 4);
    addVConverter(TID_INT8, TID_UINT8, &rle_unary_func<UnaryConverter, Int8, Uint8>, 1);
    addVConverter(TID_INT8, TID_UINT16, &rle_unary_func<UnaryConverter, Int8, Uint16>, 2);
    addVConverter(TID_INT8, TID_UINT32, &rle_unary_func<UnaryConverter, Int8, Uint32>, 3);
    addVConverter(TID_INT8, TID_UINT64, &rle_unary_func<UnaryConverter, Int8, Uint64>, 4);
    addVConverter(TID_INT8, TID_FLOAT, &rle_unary_func<UnaryConverter, Int8, Float>, TRANSFORM_CONVERSION_COST*2);
    addVConverter(TID_INT8, TID_DOUBLE, &rle_unary_func<UnaryConverter, Int8, Double>, TRANSFORM_CONVERSION_COST);
    addVConverter(TID_INT8, TID_BOOL, &rle_unary_func<UnaryConverter, Int8, Bool>, TRANSFORM_CONVERSION_COST);

    // TID_INT16
    addVFunction(FunctionDescription("+", list_of(TID_INT16)(TID_INT16), TypeId(TID_INT16), &rle_binary_func<BinaryPlus, Int16, Int16, Int16>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_INT16)(TID_INT16), TypeId(TID_INT16), &rle_binary_func<BinaryMinus, Int16, Int16, Int16>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_INT16)(TID_INT16), TypeId(TID_INT16), &rle_binary_func<BinaryMult, Int16, Int16, Int16>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_INT16)(TID_INT16), TypeId(TID_INT16), &rle_binary_func<BinaryDiv, Int16, Int16, Int16>, 0));
    addVFunction(FunctionDescription("%", list_of(TID_INT16)(TID_INT16), TypeId(TID_INT16), &rle_binary_func<BinaryMod, Int16, Int16, Int16>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_INT16)(TID_INT16), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Int16, Int16, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_INT16)(TID_INT16), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Int16, Int16, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_INT16)(TID_INT16), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Int16, Int16, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_INT16)(TID_INT16), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Int16, Int16, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_INT16)(TID_INT16), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Int16, Int16, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_INT16)(TID_INT16), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Int16, Int16, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_INT16), TypeId(TID_INT16), &rle_unary_func<UnaryMinus, Int16, Int16>, 0));

    addVConverter(TID_INT16, TID_INT8, &rle_unary_func<UnaryConverter, Int16, Int8>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_INT16, TID_INT32, &rle_unary_func<UnaryConverter, Int16, Int32>, 2);
    addVConverter(TID_INT16, TID_INT64, &rle_unary_func<UnaryConverter, Int16, Int64>, 3);
    addVConverter(TID_INT16, TID_UINT8, &rle_unary_func<UnaryConverter, Int16, Uint8>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_INT16, TID_UINT16, &rle_unary_func<UnaryConverter, Int16, Uint16>, 1);
    addVConverter(TID_INT16, TID_UINT32, &rle_unary_func<UnaryConverter, Int16, Uint32>, 2);
    addVConverter(TID_INT16, TID_UINT64, &rle_unary_func<UnaryConverter, Int16, Uint64>, 3);
    addVConverter(TID_INT16, TID_FLOAT, &rle_unary_func<UnaryConverter, Int16, Float>, TRANSFORM_CONVERSION_COST*2);
    addVConverter(TID_INT16, TID_DOUBLE, &rle_unary_func<UnaryConverter, Int16, Double>, TRANSFORM_CONVERSION_COST);
    addVConverter(TID_INT16, TID_BOOL, &rle_unary_func<UnaryConverter, Int16, Bool>, TRANSFORM_CONVERSION_COST);

    /* TID_INT32 */
    addVFunction(FunctionDescription("+", list_of(TID_INT32)(TID_INT32), TypeId(TID_INT32), &rle_binary_func<BinaryPlus, Int32, Int32, Int32>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_INT32)(TID_INT32), TypeId(TID_INT32), &rle_binary_func<BinaryMinus, Int32, Int32, Int32>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_INT32)(TID_INT32), TypeId(TID_INT32), &rle_binary_func<BinaryMult, Int32, Int32, Int32>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_INT32)(TID_INT32), TypeId(TID_INT32), &rle_binary_func<BinaryDiv, Int32, Int32, Int32>, 0));
    addVFunction(FunctionDescription("%", list_of(TID_INT32)(TID_INT32), TypeId(TID_INT32), &rle_binary_func<BinaryMod, Int32, Int32, Int32>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_INT32)(TID_INT32), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Int32, Int32, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_INT32)(TID_INT32), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Int32, Int32, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_INT32)(TID_INT32), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Int32, Int32, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_INT32)(TID_INT32), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Int32, Int32, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_INT32)(TID_INT32), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Int32, Int32, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_INT32)(TID_INT32), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Int32, Int32, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_INT32), TypeId(TID_INT32), &rle_unary_func<UnaryMinus, Int32, Int32>, 0));

    addVConverter(TID_INT32, TID_INT8, &rle_unary_func<UnaryConverter, Int32, Int8>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_INT32, TID_INT16, &rle_unary_func<UnaryConverter, Int32, Int16>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_INT32, TID_INT64, &rle_unary_func<UnaryConverter, Int32, Int64>, 2);
    addVConverter(TID_INT32, TID_UINT8, &rle_unary_func<UnaryConverter, Int32, Uint8>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_INT32, TID_UINT16, &rle_unary_func<UnaryConverter, Int32, Uint16>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_INT32, TID_UINT32, &rle_unary_func<UnaryConverter, Int32, Uint32>, 1);
    addVConverter(TID_INT32, TID_UINT64, &rle_unary_func<UnaryConverter, Int32, Uint64>, 2);
    addVConverter(TID_INT32, TID_FLOAT, &rle_unary_func<UnaryConverter, Int32, Float>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_INT32, TID_DOUBLE, &rle_unary_func<UnaryConverter, Int32, Double>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_INT32, TID_BOOL, &rle_unary_func<UnaryConverter, Int32, Bool>, TRANSFORM_CONVERSION_COST);

    /* TID_INT64 */
    addVFunction(FunctionDescription("+", list_of(TID_INT64)(TID_INT64), TypeId(TID_INT64), &rle_binary_func<BinaryPlus, Int64, Int64, Int64>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_INT64)(TID_INT64), TypeId(TID_INT64), &rle_binary_func<BinaryMinus, Int64, Int64, Int64>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_INT64)(TID_INT64), TypeId(TID_INT64), &rle_binary_func<BinaryMult, Int64, Int64, Int64>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_INT64)(TID_INT64), TypeId(TID_INT64), &rle_binary_func<BinaryDiv, Int64, Int64, Int64>, 0));
    addVFunction(FunctionDescription("%", list_of(TID_INT64)(TID_INT64), TypeId(TID_INT64), &rle_binary_func<BinaryMod, Int64, Int64, Int64>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_INT64)(TID_INT64), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Int64, Int64, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_INT64)(TID_INT64), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Int64, Int64, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_INT64)(TID_INT64), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Int64, Int64, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_INT64)(TID_INT64), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Int64, Int64, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_INT64)(TID_INT64), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Int64, Int64, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_INT64)(TID_INT64), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Int64, Int64, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_INT64), TypeId(TID_INT64), &rle_unary_func<UnaryMinus, Int64, Int64>, 0));

    addVConverter(TID_INT64, TID_INT8, &rle_unary_func<UnaryConverter, Int64, Int8>, TRUNCATE_CONVERSION_COST*3);
    addVConverter(TID_INT64, TID_INT16, &rle_unary_func<UnaryConverter, Int64, Int16>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_INT64, TID_INT32, &rle_unary_func<UnaryConverter, Int64, Int32>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_INT64, TID_UINT8, &rle_unary_func<UnaryConverter, Int64, Uint8>, TRUNCATE_CONVERSION_COST*3);
    addVConverter(TID_INT64, TID_UINT16, &rle_unary_func<UnaryConverter, Int64, Uint16>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_INT64, TID_UINT32, &rle_unary_func<UnaryConverter, Int64, Uint32>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_INT64, TID_UINT64, &rle_unary_func<UnaryConverter, Int64, Uint64>, 1);
    addVConverter(TID_INT64, TID_FLOAT, &rle_unary_func<UnaryConverter, Int64, Float>, TRANSFORM_CONVERSION_COST*2);
    addVConverter(TID_INT64, TID_DOUBLE, &rle_unary_func<UnaryConverter, Int64, Double>, TRANSFORM_CONVERSION_COST);
    addVConverter(TID_INT64, TID_BOOL, &rle_unary_func<UnaryConverter, Int64, Bool>, TRANSFORM_CONVERSION_COST);

    /* TID_DATETIME */
    // DateTime - Int64 = DateTime: means datatime reduced on int64 number seconds
    addVFunction(FunctionDescription("-", list_of(TID_DATETIME)(TID_INT64), TypeId(TID_DATETIME), &rle_binary_func<BinaryMinus, DateTime, Int64, DateTime>, 0));
    // DataTime - DateTime = Int64: means number of seconds between datetimes
    addVFunction(FunctionDescription("-", list_of(TID_DATETIME)(TID_DATETIME), TypeId(TID_INT64), &rle_binary_func<BinaryMinus, DateTime, DateTime, Int64>, 0));
    addVFunction(FunctionDescription("=", list_of(TID_DATETIME)(TID_DATETIME), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, DateTime, DateTime, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_DATETIME)(TID_DATETIME), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, DateTime, DateTime, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_DATETIME)(TID_DATETIME), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, DateTime, DateTime, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_DATETIME)(TID_DATETIME), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, DateTime, DateTime, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_DATETIME)(TID_DATETIME), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, DateTime, DateTime, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_DATETIME)(TID_DATETIME), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, DateTime, DateTime, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_DATETIME), TypeId(TID_DATETIME), &rle_unary_func<UnaryMinus, DateTime, DateTime>, 0));

    addVConverter(TID_DATETIME, TID_INT64, &rle_unary_func<UnaryConverter, DateTime, Int64>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DATETIME, TID_DOUBLE, &rle_unary_func<UnaryConverter, DateTime, Double>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_INT64, TID_DATETIME, &rle_unary_func<UnaryConverter, Int64, DateTime>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_DATETIME, &rle_unary_func<UnaryConverter, Double, DateTime>, EXPLICIT_CONVERSION_COST);

    // TID_UINT8
    addVFunction(FunctionDescription("+", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_UINT8), &rle_binary_func<BinaryPlus, Uint8, Uint8, Uint8>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_UINT8), &rle_binary_func<BinaryMinus, Uint8, Uint8, Uint8>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_UINT8), &rle_binary_func<BinaryMult, Uint8, Uint8, Uint8>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_UINT8), &rle_binary_func<BinaryDiv, Uint8, Uint8, Uint8>, 0));
    addVFunction(FunctionDescription("%", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_UINT8), &rle_binary_func<BinaryMod, Uint8, Uint8, Uint8>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Uint8, Uint8, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Uint8, Uint8, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Uint8, Uint8, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Uint8, Uint8, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Uint8, Uint8, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_UINT8)(TID_UINT8), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Uint8, Uint8, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_UINT8), TypeId(TID_UINT8), &rle_unary_func<UnaryMinus, Uint8, Uint8>, 0));

    addVConverter(TID_UINT8, TID_UINT16, &rle_unary_func<UnaryConverter, Uint8, Uint16>, 2);
    addVConverter(TID_UINT8, TID_UINT32, &rle_unary_func<UnaryConverter, Uint8, Uint32>, 3);
    addVConverter(TID_UINT8, TID_UINT64, &rle_unary_func<UnaryConverter, Uint8, Uint64>, 4);
    addVConverter(TID_UINT8, TID_INT8, &rle_unary_func<UnaryConverter, Uint8, Int8>, 1);
    addVConverter(TID_UINT8, TID_INT16, &rle_unary_func<UnaryConverter, Uint8, Int16>, 2);
    addVConverter(TID_UINT8, TID_INT32, &rle_unary_func<UnaryConverter, Uint8, Int32>, 3);
    addVConverter(TID_UINT8, TID_INT64, &rle_unary_func<UnaryConverter, Uint8, Int64>, 4);
    addVConverter(TID_UINT8, TID_FLOAT, &rle_unary_func<UnaryConverter, Uint8, Float>, TRANSFORM_CONVERSION_COST*2);
    addVConverter(TID_UINT8, TID_DOUBLE, &rle_unary_func<UnaryConverter, Uint8, Double>, TRANSFORM_CONVERSION_COST);
    addVConverter(TID_UINT8, TID_BOOL, &rle_unary_func<UnaryConverter, Uint8, Bool>, TRANSFORM_CONVERSION_COST);

    // TID_UINT16
    addVFunction(FunctionDescription("+", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_UINT16), &rle_binary_func<BinaryPlus, Uint16, Uint16, Uint16>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_UINT16), &rle_binary_func<BinaryMinus, Uint16, Uint16, Uint16>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_UINT16), &rle_binary_func<BinaryMult, Uint16, Uint16, Uint16>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_UINT16), &rle_binary_func<BinaryDiv, Uint16, Uint16, Uint16>, 0));
    addVFunction(FunctionDescription("%", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_UINT16), &rle_binary_func<BinaryMod, Uint16, Uint16, Uint16>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Uint16, Uint16, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Uint16, Uint16, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Uint16, Uint16, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Uint16, Uint16, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Uint16, Uint16, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_UINT16)(TID_UINT16), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Uint16, Uint16, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_UINT16), TypeId(TID_UINT16), &rle_unary_func<UnaryMinus, Uint16, Uint16>, 0));

    addVConverter(TID_UINT16, TID_UINT8, &rle_unary_func<UnaryConverter, Uint16, Uint8>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_UINT16, TID_UINT32, &rle_unary_func<UnaryConverter, Uint16, Uint32>, 2);
    addVConverter(TID_UINT16, TID_UINT64, &rle_unary_func<UnaryConverter, Uint16, Uint64>, 3);
    addVConverter(TID_UINT16, TID_INT8, &rle_unary_func<UnaryConverter, Uint16, Int8>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_UINT16, TID_INT16, &rle_unary_func<UnaryConverter, Uint16, Int16>, 1);
    addVConverter(TID_UINT16, TID_INT32, &rle_unary_func<UnaryConverter, Uint16, Int32>, 2);
    addVConverter(TID_UINT16, TID_INT64, &rle_unary_func<UnaryConverter, Uint16, Int64>, 3);
    addVConverter(TID_UINT16, TID_FLOAT, &rle_unary_func<UnaryConverter, Uint16, Float>, TRANSFORM_CONVERSION_COST*2);
    addVConverter(TID_UINT16, TID_DOUBLE, &rle_unary_func<UnaryConverter, Uint16, Double>, TRANSFORM_CONVERSION_COST);
    addVConverter(TID_UINT16, TID_BOOL, &rle_unary_func<UnaryConverter, Uint16, Bool>, TRANSFORM_CONVERSION_COST);

    // TID_UINT32
    addVFunction(FunctionDescription("+", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_UINT32), &rle_binary_func<BinaryPlus, Uint32, Uint32, Uint32>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_UINT32), &rle_binary_func<BinaryMinus, Uint32, Uint32, Uint32>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_UINT32), &rle_binary_func<BinaryMult, Uint32, Uint32, Uint32>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_UINT32), &rle_binary_func<BinaryDiv, Uint32, Uint32, Uint32>, 0));
    addVFunction(FunctionDescription("%", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_UINT32), &rle_binary_func<BinaryMod, Uint32, Uint32, Uint32>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Uint32, Uint32, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Uint32, Uint32, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Uint32, Uint32, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Uint32, Uint32, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Uint32, Uint32, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_UINT32)(TID_UINT32), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Uint32, Uint32, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_UINT32), TypeId(TID_UINT32), &rle_unary_func<UnaryMinus, Uint32, Uint32>, 0));

    addVConverter(TID_UINT32, TID_UINT8, &rle_unary_func<UnaryConverter, Uint32, Uint8>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_UINT32, TID_UINT16, &rle_unary_func<UnaryConverter, Uint32, Uint16>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_UINT32, TID_UINT64, &rle_unary_func<UnaryConverter, Uint32, Uint64>, 2);
    addVConverter(TID_UINT32, TID_INT8, &rle_unary_func<UnaryConverter, Uint32, Int8>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_UINT32, TID_INT16, &rle_unary_func<UnaryConverter, Uint32, Int16>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_UINT32, TID_INT32, &rle_unary_func<UnaryConverter, Uint32, Int32>, 1);
    addVConverter(TID_UINT32, TID_INT64, &rle_unary_func<UnaryConverter, Uint32, Int64>, 2);
    addVConverter(TID_UINT32, TID_FLOAT, &rle_unary_func<UnaryConverter, Uint32, Float>, TRANSFORM_CONVERSION_COST*2);
    addVConverter(TID_UINT32, TID_DOUBLE, &rle_unary_func<UnaryConverter, Uint32, Double>, TRANSFORM_CONVERSION_COST);
    addVConverter(TID_UINT32, TID_BOOL, &rle_unary_func<UnaryConverter, Uint32, Bool>, TRANSFORM_CONVERSION_COST);

    // TID_UINT64
    addVFunction(FunctionDescription("+", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_UINT64), &rle_binary_func<BinaryPlus, Uint64, Uint64, Uint64>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_UINT64), &rle_binary_func<BinaryMinus, Uint64, Uint64, Uint64>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_UINT64), &rle_binary_func<BinaryMult, Uint64, Uint64, Uint64>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_UINT64), &rle_binary_func<BinaryDiv, Uint64, Uint64, Uint64>, 0));
    addVFunction(FunctionDescription("%", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_UINT64), &rle_binary_func<BinaryMod, Uint64, Uint64, Uint64>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Uint64, Uint64, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Uint64, Uint64, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Uint64, Uint64, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Uint64, Uint64, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Uint64, Uint64, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_UINT64)(TID_UINT64), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Uint64, Uint64, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_UINT64), TypeId(TID_UINT64), &rle_unary_func<UnaryMinus, Uint64, Uint64>, 0));

    addVConverter(TID_UINT64, TID_UINT8, &rle_unary_func<UnaryConverter, Uint64, Uint8>, TRUNCATE_CONVERSION_COST*3);
    addVConverter(TID_UINT64, TID_UINT16, &rle_unary_func<UnaryConverter, Uint64, Uint16>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_UINT64, TID_UINT32, &rle_unary_func<UnaryConverter, Uint64, Uint32>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_UINT64, TID_INT8, &rle_unary_func<UnaryConverter, Uint64, Int8>, TRUNCATE_CONVERSION_COST*3);
    addVConverter(TID_UINT64, TID_INT16, &rle_unary_func<UnaryConverter, Uint64, Int16>, TRUNCATE_CONVERSION_COST*2);
    addVConverter(TID_UINT64, TID_INT32, &rle_unary_func<UnaryConverter, Uint64, Int32>, TRUNCATE_CONVERSION_COST);
    addVConverter(TID_UINT64, TID_INT64, &rle_unary_func<UnaryConverter, Uint64, Int64>, 1);
    addVConverter(TID_UINT64, TID_FLOAT, &rle_unary_func<UnaryConverter, Uint64, Float>, TRANSFORM_CONVERSION_COST*2);
    addVConverter(TID_UINT64, TID_DOUBLE, &rle_unary_func<UnaryConverter, Uint64, Double>, TRANSFORM_CONVERSION_COST);
    addVConverter(TID_UINT64, TID_BOOL, &rle_unary_func<UnaryConverter, Uint64, Bool>, TRANSFORM_CONVERSION_COST);

    /* TID_FLOAT */
    addVFunction(FunctionDescription("+", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_FLOAT), &rle_binary_func<BinaryPlus, Float, Float, Float>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_FLOAT), &rle_binary_func<BinaryMinus, Float, Float, Float>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_FLOAT), &rle_binary_func<BinaryMult, Float, Float, Float>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_FLOAT), &rle_binary_func<BinaryDiv, Float, Float, Float>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Float, Float, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Float, Float, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Float, Float, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Float, Float, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Float, Float, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Float, Float, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_FLOAT), TypeId(TID_FLOAT), &rle_unary_func<UnaryMinus, Float, Float>, 0));

    addVConverter(TID_FLOAT, TID_DOUBLE, &rle_unary_func<UnaryConverter, Float, Double>, 1);
    addVConverter(TID_FLOAT, TID_CHAR, &rle_unary_func<UnaryConverter, Float, Char>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_FLOAT, TID_INT8, &rle_unary_func<UnaryConverter, Float, Int8>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_FLOAT, TID_INT16, &rle_unary_func<UnaryConverter, Float, Int16>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_FLOAT, TID_INT32, &rle_unary_func<UnaryConverter, Float, Int32>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_FLOAT, TID_INT64, &rle_unary_func<UnaryConverter, Float, Int64>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_FLOAT, TID_UINT8, &rle_unary_func<UnaryConverter, Float, Uint8>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_FLOAT, TID_UINT16, &rle_unary_func<UnaryConverter, Float, Uint16>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_FLOAT, TID_UINT32, &rle_unary_func<UnaryConverter, Float, Uint32>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_FLOAT, TID_UINT64, &rle_unary_func<UnaryConverter, Float, Uint64>, EXPLICIT_CONVERSION_COST);

    /* TID_DOUBLE */
    addVFunction(FunctionDescription("+", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_DOUBLE), &rle_binary_func<BinaryPlus, Double, Double, Double>, 0));
    addVFunction(FunctionDescription("-", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_DOUBLE), &rle_binary_func<BinaryMinus, Double, Double, Double>, 0));
    addVFunction(FunctionDescription("*", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_DOUBLE), &rle_binary_func<BinaryMult, Double, Double, Double>, 0));
    addVFunction(FunctionDescription("/", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_DOUBLE), &rle_binary_func<BinaryDiv, Double, Double, Double>, 0));

    addVFunction(FunctionDescription("=", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Double, Double, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Double, Double, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Double, Double, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Double, Double, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Double, Double, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Double, Double, Bool>, 0));

    addVFunction(FunctionDescription("-", list_of(TID_DOUBLE), TypeId(TID_DOUBLE), &rle_unary_func<UnaryMinus, Double, Double>, 0));

    addVConverter(TID_DOUBLE, TID_FLOAT, &rle_unary_func<UnaryConverter, Double, Float>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_CHAR, &rle_unary_func<UnaryConverter, Double, Char>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_INT8, &rle_unary_func<UnaryConverter, Double, Int8>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_INT16, &rle_unary_func<UnaryConverter, Double, Int16>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_INT32, &rle_unary_func<UnaryConverter, Double, Int32>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_INT64, &rle_unary_func<UnaryConverter, Double, Int64>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_UINT8, &rle_unary_func<UnaryConverter, Double, Uint8>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_UINT16, &rle_unary_func<UnaryConverter, Double, Uint16>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_UINT32, &rle_unary_func<UnaryConverter, Double, Uint32>, EXPLICIT_CONVERSION_COST);
    addVConverter(TID_DOUBLE, TID_UINT64, &rle_unary_func<UnaryConverter, Double, Uint64>, EXPLICIT_CONVERSION_COST);

    /* TID_BOOL */
    // TODO: Boolean operations can be implemented in more efficient way by using bit operations.
    addVFunction(FunctionDescription("=", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &rle_binary_func<BinaryEq, Bool, Bool, Bool>, 0));
    addVFunction(FunctionDescription("<", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &rle_binary_func<BinaryLess, Bool, Bool, Bool>, 0));
    addVFunction(FunctionDescription("<=", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &rle_binary_func<BinaryLessOrEq, Bool, Bool, Bool>, 0));
    addVFunction(FunctionDescription("<>", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &rle_binary_func<BinaryNotEq, Bool, Bool, Bool>, 0));
    addVFunction(FunctionDescription(">=", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &rle_binary_func<BinaryGreaterOrEq, Bool, Bool, Bool>, 0));
    addVFunction(FunctionDescription(">", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &rle_binary_func<BinaryGreater, Bool, Bool, Bool>, 0));
    // TODO: and/or operations may require specific processing of NULL operands, so maybe it's better to implement separate functions for them instead of using template
    addVFunction(FunctionDescription("and", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &rle_binary_func<BinaryAnd, Bool, Bool, Bool>, 0));
    addVFunction(FunctionDescription("or", list_of(TID_BOOL)(TID_BOOL), TypeId(TID_BOOL), &rle_binary_func<BinaryOr, Bool, Bool, Bool>, 0));

    addVFunction(FunctionDescription("not", list_of(TID_BOOL), TypeId(TID_BOOL), &rle_unary_bool_not, 0));

    /* TID_STRING */
    addVFunction(FunctionDescription("+", list_of(TID_STRING)(TID_STRING), TypeId(TID_STRING), &rle_binary_func<BinaryStringPlus, VarValue, VarValue, VarValue>, 0));

    /* Cconvertors to/from string */
//    CONVERTOR_TO_STR(TID_INT8, Int8)
//    CONVERTOR_TO_STR(TID_INT16, Int16)
//    CONVERTOR_TO_STR(TID_INT32, Int32)
//    CONVERTOR_TO_STR(TID_INT64, Int64)
//    CONVERTOR_TO_STR(TID_UINT8, Uint8)
//    CONVERTOR_TO_STR(TID_UINT16, Uint16)
//    CONVERTOR_TO_STR(TID_UINT32, Uint32)
//    CONVERTOR_TO_STR(TID_UINT64, Uint64)
//    CONVERTOR_TO_STR(TID_FLOAT, Float)
//    CONVERTOR_TO_STR(TID_DOUBLE, Double)

//    CONVERTOR_FROM_STR(TID_INT8, Int8)
//    CONVERTOR_FROM_STR(TID_INT16, Int16)
//    CONVERTOR_FROM_STR(TID_INT32, Int32)
//    CONVERTOR_FROM_STR(TID_INT64, Int64)
//    CONVERTOR_FROM_STR(TID_UINT8, Uint8)
//    CONVERTOR_FROM_STR(TID_UINT16, Uint16)
//    CONVERTOR_FROM_STR(TID_UINT32, Uint32)
//    CONVERTOR_FROM_STR(TID_UINT64, Uint64)
//    CONVERTOR_FROM_STR(TID_FLOAT, Float)
//    CONVERTOR_FROM_STR(TID_DOUBLE, Double)


    /* FUNCTIONS with 1 agrument */
    addVFunction(FunctionDescription("sin", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&sin>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("sin", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&sin>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("cos", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&cos>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("cos", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&cos>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("tan", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&tan>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("tan", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&tan>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("asin", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&asin>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("asin", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&asin>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("acos", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&acos>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("acos", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&acos>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("atan", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&atan>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("atan", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&atan>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("sqrt", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&sqrt>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("sqrt", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&sqrt>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("log", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&log>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("log", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&log>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("log10", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&log10>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("log10", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&log10>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("exp", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&exp>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("exp", list_of(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_unary_func<UnaryFunctionCall<Float, Float>::Function<&exp>::Op, Float, Float>, 0));
    addVFunction(FunctionDescription("ceil", list_of(TID_DOUBLE), TypeId(TID_INT64),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&ceil>::Op, Double, Int64>, 0));
    addVFunction(FunctionDescription("floor", list_of(TID_DOUBLE), TypeId(TID_INT64),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&floor>::Op, Double, Int64>, 0));
    addVFunction(FunctionDescription("abs", list_of(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_unary_func<UnaryFunctionCall<Double, Double>::Function<&abs>::Op, Double, Double>, 0));
    addVFunction(FunctionDescription("abs", list_of(TID_INT32), TypeId(TID_INT32),
            &rle_unary_func<UnaryFunctionCall<Int32, Int32>::Function<&abs>::Op, Int32, Int32>, 0));

    /* FUNCTIONS with 2 agruments */
    addVFunction(FunctionDescription("atan2", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_binary_func<BinaryFunctionCall<Double, Double, Double>::Function<&atan2>::Op, Double, Double, Double>, 0));
    addVFunction(FunctionDescription("atan2", list_of(TID_FLOAT)(TID_FLOAT), TypeId(TID_FLOAT),
            &rle_binary_func<BinaryFunctionCall<Float, Float, Float>::Function<&atan2>::Op, Float, Float, Float>, 0));
    addVFunction(FunctionDescription("pow", list_of(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_DOUBLE),
            &rle_binary_func<BinaryFunctionCall<Double, Double, Double>::Function<&pow>::Op, Double, Double, Double>, 0));

    /* Polymorphic functions */
    addVFunction(FunctionDescription("is_null", list_of(TID_VOID), TID_VOID, &rle_unary_bool_is_null, 0, false, &inferIsNullArgTypes));
}

bool FunctionLibrary::_findFunction(const std::string& name,
                                   const std::vector<TypeId>& inputArgTypes,
                                   FunctionDescription& funcDescription,
                                   std::vector<FunctionPointer>& converters,
                                   bool tile, ConversionCost& convCost, bool& swapInputs)
{
    convCost = 0;
    // Hard coded iif function because it has no parameter types except the
    // first bool. So, we create a special FunctionDescription, and return it.
    string lowCaseName = name;
    transform(lowCaseName.begin(), lowCaseName.end(), lowCaseName.begin(), ::tolower);
    if (!tile && lowCaseName == "iif") {
        if (inputArgTypes.size() == 3 && inputArgTypes[0] == TID_BOOL) {
            std::vector< TypeId> inputTypes = inputArgTypes;
            TypeId outputType;
            if (inputArgTypes[1] != inputArgTypes[2]) {
                if (Type::isSubtype(inputArgTypes[1], inputArgTypes[2])) {
                    outputType = inputArgTypes[2];
                } else if (Type::isSubtype(inputArgTypes[2], inputArgTypes[1])) {
                    outputType = inputArgTypes[1];
                } else {
                    converters.resize(inputTypes.size());
                    ConversionCost cost[3] = {EXPLICIT_CONVERSION_COST, EXPLICIT_CONVERSION_COST, EXPLICIT_CONVERSION_COST };
                    converters[1] = FunctionLibrary::getInstance()->findConverter(inputArgTypes[1], inputArgTypes[2], tile, false, &cost[1]);
                    converters[2] = FunctionLibrary::getInstance()->findConverter(inputArgTypes[2], inputArgTypes[1], tile, false, &cost[2]);
                    if (converters[1]) {
                        if (converters[2] && cost[2] < cost[1]) {
                            converters[1] = NULL;
                            convCost = cost[2];
                            outputType = inputTypes[2] = inputArgTypes[1];
                        } else {
                            outputType = inputTypes[1] = inputArgTypes[2];
                            converters[2] = NULL;
                            convCost = cost[1];
                        }
                    } else if (converters[2]) {
                        outputType = inputTypes[2] = inputArgTypes[1];
                        convCost = cost[2];
                    } else {
                        return false;
                    }
                }
            }
            else {
                outputType = inputArgTypes[1];
            }
            funcDescription = FunctionDescription ("iif", inputTypes, outputType, &iif);
            return true;
        }
        else {
            return false;
        }
    }

    if (!tile && lowCaseName == "missing_reason") {
        if (inputArgTypes.size() == 1) {
            funcDescription = FunctionDescription("missing_reason", inputArgTypes, TID_INT32, &missingReason);
            return true;
        }
        else {
            return false;
        }
    }

    if (TypeLibrary::hasType(lowCaseName) && inputArgTypes.size() == 1) {
        // Here we have explicit type converter and must try to find converter from input type to given
        const Type& dstType = TypeLibrary::getType(lowCaseName);
        ConversionCost cost = ConversionCost(~0);
        if (FunctionPointer f = FunctionLibrary::getInstance()->findConverter(inputArgTypes[0], dstType.typeId(), tile, false, &cost)) {
            funcDescription = FunctionDescription(lowCaseName, inputArgTypes, dstType.typeId(), f, 0);
            converters.clear();
            return true;
        }

    }

    funcDescNamesMap::iterator funcMap = getFunctionMap(tile).find(lowCaseName);

    if (funcMap == getFunctionMap(tile).end())
        return false;

    std::map<std::vector<TypeId>, FunctionDescription, __lesscasecmp>::iterator func = funcMap->second.find(inputArgTypes);
    if (func != funcMap->second.end() && (!swapInputs || func->second.isCommulative()) && (!func->second.getInferFunctionArgTypes())) {
        // This is full matching. Return result.
        funcDescription = func->second;
        converters.clear();
        return true;
    }

    bool foundMatch = false;
    // We have no found a function with the specified argument types. Trying
    // to find with converters.
    vector<ArgTypes> possibleArgTypes;
    vector<TypeId> possibleResultTypes;
    for (func = funcMap->second.begin(); func != funcMap->second.end(); ++func)
    {
        if (func->first.size() != inputArgTypes.size() || (swapInputs && !func->second.isCommulative()))
            continue;
        InferFunctionArgTypes inferFunc = func->second.getInferFunctionArgTypes();
        if (inferFunc) {
            inferFunc(inputArgTypes, possibleArgTypes, possibleResultTypes);
        } else {
            possibleArgTypes.resize(1);
            possibleArgTypes[0] = func->first;
            possibleResultTypes.resize(1);
            possibleResultTypes[0] = func->second.getOutputArg();
        }
        assert(possibleArgTypes.size() == possibleResultTypes.size());

        while (possibleArgTypes.size() > 0)
        {
            const size_t n = possibleArgTypes.size() - 1;
            ArgTypes funcTypes = possibleArgTypes[n];
            possibleArgTypes.resize(n);
            TypeId resultType = possibleResultTypes[n];
            possibleResultTypes.resize(n);
            std::vector<FunctionPointer> argConverters;
            bool canBeUsed = true;
            ConversionCost totalCost = 0;
            for (size_t i = 0; i < funcTypes.size(); i++) {
                if (inputArgTypes[i] != funcTypes[i] && !Type::isSubtype(inputArgTypes[i], funcTypes[i])) {
                    ConversionCost cost = EXPLICIT_CONVERSION_COST;
                    if (argConverters.size() == 0) {
                        argConverters.resize(inputArgTypes.size());
                    }
                    argConverters[i] = FunctionLibrary::getInstance()->findConverter(inputArgTypes[i], funcTypes[i], tile, false, &cost);
                    if (argConverters[i] == NULL) {
                        canBeUsed = false;
                        break;
                    }
                    totalCost += cost;
                }
            }
            if (canBeUsed) {
                if (!foundMatch) {
                    foundMatch = true;
                } else if (totalCost >= convCost) {
                    continue;
                }
                if (!inferFunc) {
                    funcDescription = func->second;
                } else {
                    const FunctionDescription& fd = func->second;
                    funcDescription = FunctionDescription(fd.getName(), funcTypes, resultType, fd.getFuncPtr(), fd.getScratchSize(),
                                                          fd.isCommulative(), inferFunc);
                }
                converters = argConverters;
                convCost = totalCost;
            }
        }
    }

    // Search for commulative functions with less cost
    if (!swapInputs && inputArgTypes.size() == 2 && inputArgTypes[0] != inputArgTypes[1]) {
        std::vector<TypeId> _inputArgTypes(2);
        _inputArgTypes[0] = inputArgTypes[1];
        _inputArgTypes[1] = inputArgTypes[0];
        FunctionDescription _funcDescription;
        std::vector<FunctionPointer> _converters;
        ConversionCost _convCost;
        bool _swapInputs = true;
        if (_findFunction(name, _inputArgTypes, _funcDescription, _converters, tile, _convCost, _swapInputs))
        {
            if (!foundMatch || _convCost < convCost)
            {
                funcDescription = _funcDescription;
                converters = _converters;
                convCost = _convCost;
                swapInputs = true;
                return true;
            }
        }
    }


    return foundMatch;
}

bool FunctionLibrary::hasFunction(std::string name, bool tile)
{
    return getFunctionMap(tile).find(name) != getFunctionMap(tile).end();
}

inline FunctionLibrary::Converter const* FunctionLibrary::findDirectConverter(TypeId const& srcType, TypeId const& destType, bool tile)
{
    const map<TypeId, Converter, __lesscasecmp>& srcConverters = getConverterMap(tile)[srcType];
    const map<TypeId, Converter, __lesscasecmp>::const_iterator& r = srcConverters.find(destType);
    return r == srcConverters.end() ? NULL : &r->second;
}


FunctionPointer FunctionLibrary::_findConverter(
                    TypeId const& srcType,
                    TypeId const& destType,
                    bool tile,
                    bool raiseException,
                    ConversionCost* cost)
{
    if (srcType == destType) {
        if (cost != NULL) {
            *cost = 0;
        }
        return &identicalConversion;
    }
    // TID_VOID can be only for NULL constants in expression
    if (srcType == TID_VOID) {
        if (cost != NULL) {
            *cost = 0;
        }
        return tile ? &rle_unary_null_to_any : &convNullToAny;
    }
    Converter const* cnv = findDirectConverter(srcType, destType, tile);
    if (cnv == NULL) {
        TypeId baseType = srcType;
        while ((baseType = TypeLibrary::getType(baseType).baseType()) != TID_VOID) {
            if (baseType == destType) {
                if (cost != NULL) {
                    *cost = 0;
                }
                return &identicalConversion;
            }
            if ((cnv = findDirectConverter(baseType, destType, tile)) != NULL) {
                break;
            }
        }
    }
    if (cnv == NULL) {
        TypeId baseType = destType;
        while ((baseType = TypeLibrary::getType(baseType).baseType()) != TID_VOID) {
            if (baseType == srcType) {
                if (cost != NULL) {
                    *cost = 0;
                }
                return &identicalConversion;
            }
            if ((cnv = findDirectConverter(srcType, baseType, tile)) != NULL) {
                break;
            }
        }
    }
    if (cnv == NULL) {
        if (raiseException) {
            throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_CANT_FIND_CONVERTER)
                << TypeLibrary::getType(srcType).name() << TypeLibrary::getType(destType).name();
        }
        else {
            return NULL;
        }
    }
    if (cost != NULL) {
        if (*cost <= cnv->cost) {
            if (raiseException)
            {
                throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_CANT_FIND_IMPLICIT_CONVERTER)
                    << TypeLibrary::getType(srcType).name() << TypeLibrary::getType(destType).name();
            }
            else
            {
                return NULL;
            }
        }
        *cost = cnv->cost;
    }
    return cnv->func;
}

void FunctionLibrary::functionCheck(const FunctionDescription& functionDesc)
{
#ifndef SCIDB_CLIENT
    //If function args count is 1 its signature can match some aggregate
    if (functionDesc.getInputArgs().size() == 1)
    {
        try
        {
            AggregateLibrary::getInstance()->createAggregate(
                    functionDesc.getName(),
                    TypeLibrary::getType(functionDesc.getInputArgs()[0]));
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANNOT_ADD_FUNCTION)
                << functionDesc.getName();
        }
        catch (const Exception &e)
        {
            //Looks like there are no such aggregates
        }
    }
#endif
}

void FunctionLibrary::addFunction(const FunctionDescription& functionDesc)
{
    functionCheck(functionDesc);
    // TODO: implement the check of ability to add function
    _sFunctionMap[functionDesc.getName()][functionDesc.getInputArgs()] = functionDesc;
    _functionLibraries.addObject(functionDesc.getMangleName());
}

void FunctionLibrary::addVFunction(const FunctionDescription& functionDesc)
{
    functionCheck(functionDesc);
    // TODO: implement the check of ability to add function
    _vFunctionMap[functionDesc.getName()][functionDesc.getInputArgs()] = functionDesc;
    _functionLibraries.addObject(functionDesc.getMangleName());
}

void FunctionLibrary::addVConverter( TypeId srcType,
                                    TypeId destType,
                                    FunctionPointer func,
                                    ConversionCost cost)
{
    // TODO: implement the check of ability to add converter
    Converter& cnv = _vConverterMap[srcType][destType];
    cnv.func = func;
    cnv.cost = _registeringBuiltInObjects ? cost : EXPLICIT_CONVERSION_COST;
}


} // namespace
