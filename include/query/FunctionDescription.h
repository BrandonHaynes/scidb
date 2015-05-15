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
 * @file FunctionDescription.h
 *
 * @author paul_geoffrey_brown@yahoo.com
 *
 * @brief Manager for Function Descriptions - used for UDFs and built-in funcs
 */

#ifndef __FUNCTION_DESCRIPTION_H__
#define __FUNCTION_DESCRIPTION_H__

#include <iostream>
#include <stdarg.h>

#include <boost/serialization/vector.hpp>

#include "query/TypeSystem.h"
#include "array/Metadata.h"

namespace scidb
{

/**
 * Cost of conversion between two types
 */
typedef size_t ConversionCost;

/**
 * Cost of implicit conversion 
 */
const ConversionCost IMPLICIT_CONVERSION_COST  = 1;

/**
 * Cost of conversion causing transformation of value, like int->double
 */
const ConversionCost TRANSFORM_CONVERSION_COST = 100;

/**
 * Cost of conversion causing loose of precision, like int64->int32
 */
const ConversionCost TRUNCATE_CONVERSION_COST  = 10000; 

/**
 * Cost of explicit conversion 
 */
const ConversionCost EXPLICIT_CONVERSION_COST  = 1000000;

/**
 * Pointer type to function used in compiled expressions.
 * Types of values must be specified by registering.
 * Also may be used as value converters if there is a function with one argument
 * of one type and with specified type of the result
 */
typedef void (*FunctionPointer)(const Value** args, Value* res, void* state);

#define REGISTER_FUNCTION(_name, _argumentTypes, _returnType, _ptr) \
    static scidb::UserDefinedFunction _ptr##_func(scidb::FunctionDescription(#_name, _argumentTypes, _returnType, &_ptr))

#define REGISTER_CONVERTER(_from, _to, _cost, _ptr) \
    static scidb::UserDefinedConverter _from##_to_##_to##_converter(scidb::TypeId(#_from), scidb::TypeId(#_to), _cost, &_ptr)

#define REGISTER_TYPE(_name, _size) \
    static scidb::UserDefinedType _name##_type(scidb::Type(#_name, _size*8))

#define REGISTER_SUBTYPE(_name, _size, _base)                                \
    static scidb::UserDefinedType _name##_type(scidb::Type(#_name, _size*8, _base))


typedef std::vector<TypeId> ArgTypes;

/**
 * Function pointer to provide scalar polymorphic function (like is_null, iif) by information about possible argument data types.
 * It takes: vector of fact arguments and output a list of vectors of possible argument data types and list of related result data types.
 */
typedef void (*InferFunctionArgTypes)(const ArgTypes&, std::vector<ArgTypes>&, std::vector<TypeId>&);

class FunctionDescription
{
private: 

    //
    // Basic properties of the function. Consider three example functions:
    //   foo ( int32, int32 ) -> int32
    //   bar ( string ) -> UDT
    //   random () -> double
    //
    std::string	_name;   // Name as appears in syntax. One
                         // of "foo", "bar" or "random".
    ArgTypes _inputArgs; // Input Argument list:
                                // { int32, int32 } for "foo"
                                // { string } for "bar"
                                // { random } for "random"

    ArgTypes _outputArgs; // Output argument list.
                                // { int32 } for "foo"
                                // { UDT.name() } for "UDT"
                                // { double } for random()

    FunctionPointer _func_ptr;  // FunctionPointer

    size_t  _scratchSize;   // Memory in bytes that allocated
                                // for use of the UDF. Would be
                                // 0 for foo() and bar(), and
                                // 4 for random() (seed).

    bool    _isInternal;    // true iff. this function is not
                                // callable directly. ie. If this is
                                // a UDF used inside another extension-
                                // like an aggregate.

    bool    _isDeterministic;   // true for both "foo" and "bar". false for "random"

    bool    _needsFinalCall;    // true if the UDF needs to be called
                                // once more with the POST_FINAL
                                // call type.

    bool _commutativity; // true for binary commutative operations like int + int

    InferFunctionArgTypes _inferFunctionArgTypes; // NULL for polymorphic function. It's used in find function.

public:
    FunctionDescription():
        _name( std::string("null")),
        _func_ptr ( NULL ),
        _scratchSize ( 0 ),
        _isInternal ( true ),
        _isDeterministic ( false ),
        _needsFinalCall ( false ),
        _commutativity(false),
        _inferFunctionArgTypes(NULL)
    {}

    FunctionDescription(const std::string& name, const ArgTypes& inputArgs,
                        TypeId outputArg, FunctionPointer func_ptr) :
        _name(name),
        _inputArgs(inputArgs),
        _outputArgs(std::vector<TypeId>(1, outputArg)),
        _func_ptr(func_ptr),
        _scratchSize(0),
        _isInternal(false),
        _isDeterministic(true),
        _needsFinalCall(false),
        _commutativity(false),
        _inferFunctionArgTypes(NULL)
    {
        assert(!_commutativity || inputArgs.size() == 2);

        if (name == "random") {
            std::cerr << "random's _isDeterministic is: " << _isDeterministic << std::endl;
        }
    }

    FunctionDescription(const std::string& name, const ArgTypes& inputArgs,
                        TypeId outputArg, FunctionPointer func_ptr,
                        size_t scratchSize) :
        _name(name),
        _inputArgs(inputArgs),
        _outputArgs(std::vector<TypeId>(1, outputArg)),
        _func_ptr(func_ptr),
        _scratchSize(scratchSize),
        _isInternal(false),
        _isDeterministic(true),
        _needsFinalCall(false),
        _commutativity(false),
        _inferFunctionArgTypes(NULL)
    {
        assert(!_commutativity || inputArgs.size() == 2);

        if (name == "random") {
            std::cerr << "random's _isDeterministic is: " << _isDeterministic << std::endl;
        }
    }

    FunctionDescription(const std::string& name, const ArgTypes& inputArgs,
            TypeId outputArg, FunctionPointer func_ptr, size_t scratchSize,
            bool commulativity, InferFunctionArgTypes inferFunctionArgTypes,
            bool isDeterministic=true):
        _name(name),
        _inputArgs(inputArgs),
        _outputArgs(std::vector<TypeId>(1, outputArg)),
        _func_ptr(func_ptr),
        _scratchSize(scratchSize),
        _isInternal(false),
        _isDeterministic(isDeterministic),
        _needsFinalCall(false),
        _commutativity(commulativity),
        _inferFunctionArgTypes(inferFunctionArgTypes)
    {
        assert(!_commutativity || inputArgs.size() == 2);

        if (name == "random") {
            std::cerr << "random's _isDeterministic is: " << _isDeterministic << std::endl;
        }
    }

    /**
     * Retrieve function public name.
     * @returns name of function
     */
    const std::string& getName() const { return _name; }

    std::string getMangleName() const;

    /**
     * Retrieve vector of input argument types
     * @returns std::vector<Type> passed into the function.
     */
    const ArgTypes& getInputArgs() const { return _inputArgs; }

    /**
     * Retrieve vector of output argument types
     * @returns std::vector<TypeId> passed
     */
    const ArgTypes& getOutputArgs() const { return _outputArgs; }
    TypeId getOutputArg() const { return _outputArgs[_outputArgs.size() - 1]; }

    /**
     * Retrieve size (in bytes) of the scratch pad this UDF needs
     * @returns size_t of scratch pad (memory block) passed between UDF calls
     */
    size_t getScratchSize() const { return _scratchSize; }

    /**
     * Retrieve the pointer to the function's implementation
     * @returns void * that can be cast to a function.
     */
    FunctionPointer getFuncPtr()  const
    {
        return _func_ptr;
    }

    /**
     * When the function pointer is set to NULL, the entire
     * Function is null.
     * @returns true iff. there is a non-zero _func_ptr;
     */
    bool isNull() const { return (NULL == _func_ptr); }

    /**
     * Returns true iff. the UDF is deterministic. Deterministic functions
     * always return the same result, given the same arguments. Examples of 
     * non-deterministic* functions include random number generators, and 
     * UDFs that call remote systems. 
     * @returns bool true iff. UDF is deterministic. false otherwise.
     */
    bool isDeterministic() const  { return _isDeterministic; }

    /**
     * Returns true if the UDF is "internal", meaning it cannot be invoked 
     * directly through AFL/AQL. Internal functions are used in combination
     * to implement other functionality: like { scalar } -> scalar aggregates. 
     * @returns bool true if the function cannot be addressed from AFL/AQL
     */
    bool isInternalUDF() const  { return _isInternal; }

    /**
     * Returns true of the UDF needs to be called one more time after
     * the final set of values has been passed into the function. The 
     * point being that the UDF malloc() something in the state
     * that needs to be free() after the query completes.
     */
    bool needsPostFinalCall() const  { return _needsFinalCall; }

    bool isCommulative() const
    {
        return _commutativity;
    }

    InferFunctionArgTypes getInferFunctionArgTypes() {
        return _inferFunctionArgTypes;
    }

    //
    // PGB: TO DO  - what about FuncId?
    bool operator==(const FunctionDescription& ob) const {
        return _name == ob._name && _inputArgs == ob._inputArgs && _outputArgs == ob._outputArgs;
    }
};
  
class UserDefinedFunction 
{ 
  public:
    UserDefinedFunction(FunctionDescription const& desc);
};

class UserDefinedConverter 
{
  public:
    UserDefinedConverter(TypeId from, TypeId to, ConversionCost cost, FunctionPointer ptr);
};

class UserDefinedType
{
  public:
    UserDefinedType(Type const& type);
};

std::ostream& operator<<(std::ostream& stream, const FunctionDescription& ob);
std::ostream& operator<<(std::ostream& stream, const std::vector<FunctionDescription>& ob);

} // namespace typesystem

#endif // __FUNCTION_DESCRIPTION_H__

