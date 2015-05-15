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
 * @file FunctionLibrary.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Scalar function library used in Expression.
 */

#ifndef FUNCTIONLIBRARY_H_
#define FUNCTIONLIBRARY_H_

#include <boost/serialization/vector.hpp>
#include <vector>
#include <set>
#include <map>
#include <stdarg.h>

#include "query/TypeSystem.h"
#include "query/FunctionDescription.h"
#include "array/Metadata.h"
#include "util/StringUtil.h"
#include "util/PluginObjects.h"
#include <util/Singleton.h>

namespace scidb
{

class AttributeReference;

/**
 * Pointer type to function used in compiled expressions.
 * Types of values must be specified by registering.
 * Also may be used as value converters if there is a function with one argument
 * of one type and with specified type of the result
 * state is allocated by Expression and will free automatically.
 */
//
// PGB: Moved all of this to the FunctionDescription class.
// typedef int32_t FunctionResult;
// typedef FunctionResult (*FunctionPointer)(const  Value* args,  Value& res);

typedef std::map<std::string, std::map<std::vector< TypeId>,  FunctionDescription >, __lesscasecmp> funcDescNamesMap;

typedef std::map<std::vector< TypeId>,  FunctionDescription > funcDescTypesMap;

/**
 * Class FunctionLibrary contains all functions which can be used in expressions.
 * It also can search external functions like operators in .so files.
 */
class FunctionLibrary: public Singleton<FunctionLibrary>
{
private:
    struct Converter {
        FunctionPointer func;
        ConversionCost  cost;
    };

    PluginObjects _functionLibraries;
    bool _registeringBuiltInObjects;

    Converter const* findDirectConverter(TypeId const& srcType, TypeId const& destType, bool tile);

    /**
      * Finds function with specified signature: that is, a known name, and a
      * list of input arguments. Returns a FunctionDescription object.
      *
      * If a perfect match for the function is not found, then we attempt to
      * build a match using type converters.
      *
      * @param [in] name a name of function
      * @param [in] inputArgTypes a vector of given argument types
      * @param [out] FunctionDescription - description of the function found.
      * @param [out] converters a vector of converters needed to convert
      * parameters from given types to types expected by functions (argTypes)
      *
      * @param [out] set to false if function can not be executed in vector mode
      * @param [inout] swapInputs [in] shows that method should search only commulative functions. [out] means that caller of function should swap input arguments
      * @return true is function was found.
      */
    bool _findFunction(const std::string& name,
                       const std::vector< TypeId>& inputArgTypes,
                       FunctionDescription& functDescription,
                       std::vector< FunctionPointer>& converters,
                       bool tile,
                       ConversionCost& cost,
                       bool& swapInputs);

    /**
     * Checks function before adding
     * Throws exception if something wrong
     *
     * @param functionDesc
     */
    void functionCheck(const FunctionDescription& functionDesc);

    /**
     * Finds a converter from one type to another.
     *
     * @param srcType a type of source value
     * @param destType a type of destination value
     * @param [out] supportsVectorMode set to false if function can not be executed in vector mode
     * @param raiseException true if exception should be throwed if
     * converter was not found.
     * @param [in out] in: maximal allowed conversion cost, out: actual conversion cost
     *
     * @return a pointer to converter. If converter was no function found
     * return NULL or throw exception depends on raiseException param.
     */
    FunctionPointer _findConverter(TypeId const& srcType,
                                   TypeId const& destType,
                                   bool tile,
                                   bool raiseException = true,
                                   ConversionCost* cost = NULL);

public:
    FunctionLibrary();

    void registerBuiltInFunctions();

    bool findFunction(const std::string& name,
                      const std::vector< TypeId>& inputArgTypes,
                      FunctionDescription& functDescription,
                      std::vector< FunctionPointer>& converters,
                      bool tile,
                      bool& swapInputs)
    {
        ConversionCost cost;
        return _findFunction(name, inputArgTypes, functDescription, converters, tile, cost, swapInputs);
    }

    bool findFunction(const std::string& name,
                      const std::vector< TypeId>& inputArgTypes,
                      FunctionDescription& functDescription,
                      std::vector< FunctionPointer>& converters,
                      bool tile)
    {
        ConversionCost cost;
        bool swapInputs = false;
        const bool found = _findFunction(name, inputArgTypes, functDescription, converters, tile, cost, swapInputs);
        assert(swapInputs == false); // if this assertion failure you need to replace this function call by full version and handle swapInputs correctly
        return found;
    }

    /**
     * Check if function with given name is in function library.
     * Mostly to decide in parser what we have encountered: function or
     * operator.
     */
    bool hasFunction(std::string name, bool tile);

    FunctionPointer findConverter(TypeId const& srcType,
                                  TypeId const& destType,
                                  bool tile,
                                  bool raiseException,
                                  ConversionCost* cost) {
        return _findConverter(srcType, destType, tile, raiseException, cost);
    }

    FunctionPointer findConverter(TypeId const& srcType,
                                  TypeId const& destType,
                                  bool tile)
    {
        return _findConverter(srcType, destType, tile);
    }

    FunctionPointer findConverter(TypeId const& srcType,
                                  TypeId const& destType)
    {
        return _findConverter(srcType, destType, false);
    }

    /**
     * Adds new function into the library.
     */
    void addFunction(const FunctionDescription& functionDesc);

    /**
     * Adds new vector function into the library.
     */
    void addVFunction(const FunctionDescription& functionDesc);

    /*
	** Get a handle to the map of function names -> { argtypes } ->
	** FunctionDescriptions
	*/
    funcDescNamesMap &getFunctions(bool tile = false) { return getFunctionMap(tile); }

    /**
     * The map with known functions. The key is function name. The value is a
	 * map with known function types.
     * The key is a vector of types. The value is a pair of function
	 * pointer and result type.
     */
    std::map<std::string, std::map<std::vector<TypeId>,  FunctionDescription>, __lesscasecmp> _sFunctionMap;
    std::map<std::string, std::map<std::vector<TypeId>,  FunctionDescription>, __lesscasecmp> _vFunctionMap;

    std::map<std::string, std::map<std::vector<TypeId>,  FunctionDescription>, __lesscasecmp>& getFunctionMap(bool tile)
    {
        return tile ? _vFunctionMap : _sFunctionMap;
    }

    /**
     * The map of known converters of one type to another.
	 * map[srcType][DestType]
     */
    std::map< TypeId, std::map< TypeId, Converter, __lesscasecmp>, __lesscasecmp> _sConverterMap;
    std::map< TypeId, std::map< TypeId, Converter, __lesscasecmp>, __lesscasecmp> _vConverterMap;

    std::map< TypeId, std::map< TypeId, Converter, __lesscasecmp>, __lesscasecmp>& getConverterMap(bool tile)
    {
        return tile ? _vConverterMap : _sConverterMap;
    }

    /**
     * Adds new converter into the library.
     */
    void addConverter(TypeId srcType,
                      TypeId destType,
                      FunctionPointer converter,
                      ConversionCost cost)
    {
        // TODO: implement the check of ability to add converter
        Converter& cnv = _sConverterMap[srcType][destType];
        cnv.func = converter;
        cnv.cost = _registeringBuiltInObjects ? cost : EXPLICIT_CONVERSION_COST;
    }

    /**
     * Adds new vector converter into the library.
     */
    void addVConverter(TypeId srcType, TypeId destType, FunctionPointer converter,
                      ConversionCost cost);

    const PluginObjects& getFunctionLibraries() {
        return _functionLibraries;
    }

};

} // namespace

#endif
