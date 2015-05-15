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
 * @file Expression.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <cmath>
#include <boost/format.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <cstdarg>

#include "query/QueryPlanUtilites.h"
#include "query/Expression.h"
#include "query/LogicalExpression.h"
#include "query/TypeSystem.h"
#include "system/SystemCatalog.h"
#include "array/DBArray.h"
#include "network/NetworkManager.h"
#include "query/ParsingContext.h"
#include "query/Parser.h"

using namespace std;
using namespace boost;

namespace scidb
{

// ExpressionContext implementation
ExpressionContext::ExpressionContext(Expression& expression):
    _expression(expression), _contextChanged(true)
{
    assert(_expression._compiled);

    const size_t nArgs = _expression._eargs.size();
    _args.resize(nArgs);
    _vargs.resize(_expression._tempValuesNumber);
    memset(&_args[0], 0, nArgs * sizeof(Value*));
    _context.resize(_expression._contextNo.size());

    for (size_t i = 0; i< _expression._contextNo.size(); i++) {
        const vector<size_t>& indecies = _expression._contextNo[i];
        _context[i] = expression._eargs[indecies[0]];
        for (size_t j = 0; j < indecies.size(); j++) {
            _args[indecies[j]] = &_context[i];
        }
    }

    size_t tempIndex = 0;
    for (size_t i = 0; i < nArgs; i++) {
        if (!_args[i]) {
            if (_expression._props[i].isConst) {
                _args[i] = &_expression._eargs[i];
            } else {
                _vargs[tempIndex] = _expression._eargs[i];
                _args[i] = &_vargs[tempIndex++];
            }
        }
    }

    _state.resize(expression._functions.size());
    for (size_t i = 0; i < _state.size(); i++) {
        const size_t size = expression._functions[i].stateSize;
        if (size > 0)
        {
            _state[i] = shared_array<char>(new char[size]);
            memset(_state[i].get(), 0, size);
        }
    }
}

const Value& ExpressionContext::operator[](int i) const
{
    return _context[i];
}

Value& ExpressionContext::operator[](int i)
{
    _contextChanged = true;
    return _context[i];
}

// Expression implementation
void Expression::compile(boost::shared_ptr<LogicalExpression> expr,
                         const boost::shared_ptr<Query>& query,
                         bool tile,
                         TypeId expectedType,
                         const vector< ArrayDesc>& inputSchemas,
                         const ArrayDesc& outputSchema)
{
    try
    {
        _tileMode = tile;
        for (size_t i = 0; i < _variables.size(); i++)
        {
            BindInfo b;
            b.kind = BindInfo::BI_VALUE;
            b.type = _variables[i].type;
            b.inputNo = i;
            b.resolvedId = 0;
            _bindings.push_back(b);
        }
        _contextNo.resize(_bindings.size());
        _inputSchemas = inputSchemas;
        _outputSchema = outputSchema;
        _nullable = false;
        _constant = true;
        _resultType = internalCompile(expr, query, tile, 0, 0, false).type;
        if (_resultType != expectedType && expectedType != TID_VOID) {
            ConversionCost cost = EXPLICIT_CONVERSION_COST;
            FunctionPointer converter =
                FunctionLibrary::getInstance()->findConverter(_resultType,
                                                              expectedType,
                                                              tile,
                                                              true,
                                                              &cost);
            insertConverter(expectedType, converter, 0, -1, tile);
            _resultType = expectedType;
        }
        _eargs.resize(_props.size());
        for (size_t i = 0; i < _props.size(); i++)
        {
            // Create new value with needed type if it's not a constant
            if (!_props[i].isConst) {
                Type const& t = TypeLibrary::getType(_props[i].type);
                _eargs[i] = tile ? Value(t,Value::asTile) : Value(t);
                _tempValuesNumber++;
            }
        }
        _tempValuesNumber -= _contextNo.size();
        _compiled = true;
    }
    catch (const scidb::Exception& e)
    {
        if (_tileMode &&
                (e.getLongErrorCode() == SCIDB_LE_FUNCTION_NOT_FOUND ||
                 e.getShortErrorCode() == SCIDB_SE_TYPE_CONVERSION)) {
            clear();
            compile(expr, query, false, expectedType, inputSchemas, outputSchema);
        } else {
            throw;
        }
    }
}


void Expression::compile(std::string functionName,
         bool tile,
         TypeId leftType,  TypeId rightType,
         TypeId expectedType)
{
    _tileMode = tile;
    CompiledFunction f;
    f.functionName = functionName;
    f.resultIndex = 0;
    f.argIndex = _props.size();
    size_t functionIndex = _functions.size();
    _functions.push_back(f);
    // Reserving space for new values
    _props.resize(_props.size() + 2);

    vector<TypeId> funcArgTypes;
    _props[f.argIndex].type = leftType;
    funcArgTypes.push_back(leftType);
    _props[f.argIndex + 1].type = rightType;
    funcArgTypes.push_back(rightType);

    BindInfo b;

    b.kind = BindInfo::BI_VALUE;
    b.type = leftType;
    b.value = Value(TypeLibrary::getType(leftType));
    _bindings.push_back(b);
    _contextNo.push_back(vector<size_t>(1, 1));

    b.type = rightType;
    b.value = Value(TypeLibrary::getType(rightType));
    _bindings.push_back(b);
    _contextNo.push_back(vector<size_t>(1, 2));

    _nullable = false;
    _constant = true;

    // Searching function pointer for given function name and type of arguments
    FunctionDescription functionDesc;
    vector<FunctionPointer> converters;
    if (FunctionLibrary::getInstance()->findFunction(f.functionName, funcArgTypes,
                                                     functionDesc, converters,
                                                     tile))
    {
        _functions[functionIndex].function = functionDesc.getFuncPtr();
        _resultType = functionDesc.getOutputArg();
        _functions[functionIndex].functionTypes = functionDesc.getInputArgs();
        if (functionDesc.getScratchSize() > 0)
            _functions[functionIndex].stateSize = functionDesc.getScratchSize();
        for (size_t i = 0; i < converters.size(); i++) {
            if (converters[i]) {
                insertConverter(_functions[functionIndex].functionTypes[i],
                                converters[i], f.argIndex + i, functionIndex, tile);
            }
        }
        _props[0].type = _resultType;
    }
    else
    {
        stringstream ss;
        ss << functionName << '(';
        for (size_t i = 0; i < funcArgTypes.size(); i++) {
            ss << TypeLibrary::getType(funcArgTypes[i]).name();
            if (i < funcArgTypes.size() - 1)
                ss << ", ";
        }
        ss << ')';
        throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_FUNCTION_NOT_FOUND)
            << ss.str();
    }

    if (_resultType != expectedType && expectedType != TID_VOID) {
        FunctionPointer converter = FunctionLibrary::getInstance()->findConverter(_resultType, expectedType, tile);
        if (!converter)
        {
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_TYPE_CONVERSION_ERROR)
                << TypeLibrary::getType(_resultType).name() << TypeLibrary::getType(expectedType).name();
        }
        insertConverter(expectedType, converter, 0, -1, tile);
        _resultType = expectedType;
    }
    _eargs.resize(_props.size());
    for (size_t i = 0; i < _props.size(); i++)
    {
        assert(i == 0 || _props[i].type != TID_VOID || _eargs[i].isNull());
        assert(!_props[i].isConst);
        Type const& t = TypeLibrary::getType(_props[i].type);
        _eargs[i] = tile ? Value(t,Value::asTile) : Value(t);
    }
    _tempValuesNumber = _eargs.size() - 2;
    _compiled = true;
}

void Expression::compile(const string& expression, const vector<string>& names, const vector<TypeId>& types, TypeId expectedType)
{
    assert(names.size() == types.size());

    shared_ptr<LogicalExpression> logicalExpression(parseExpression(expression));

    for (size_t i = 0; i < names.size(); i++) {
        addVariableInfo(names[i], types[i]);
    }
    shared_ptr<Query> emptyQuery; //XXX tigor TODO: expressions should not read/write data and need a query context (right?)
    compile(logicalExpression, emptyQuery, false, expectedType);
}

void Expression::compile(bool tile, const TypeId& type, const Value& value)
{
    _tileMode = tile;
    _eargs[0] = tile ? makeTileConstant(type,value) : value;
    _props[0].type = type;
    _props[0].isConst = true;
    _resultType = type;
    _compiled = true;
    _constant = true;
    _nullable = value.isNull();
}

void Expression::addVariableInfo(const std::string& name, const TypeId& type)
{
    _variables.push_back(VarInfo(name, type));
}

BindInfo
Expression::resolveContext(const ArrayDesc& arrayDesc, const string& arrayName,
                           const string& referenceName, const boost::shared_ptr<Query>& query)
{
    BindInfo bind;
    const Attributes& attrs = arrayDesc.getAttributes();
    for (size_t i = 0; i < attrs.size(); i++)
    {
        if (attrs[i].getName() == referenceName && attrs[i].hasAlias(arrayName))
        {
            bind.kind = BindInfo::BI_ATTRIBUTE;
            bind.resolvedId = i;
            bind.type = attrs[i].getType();
            _nullable |= attrs[i].isNullable();
            _constant = false;
            return bind;
        }
    }

    const Dimensions& dims = arrayDesc.getDimensions();
    for (size_t i = 0; i < dims.size(); i++) {
        if (dims[i].hasNameAndAlias(referenceName, arrayName))
        {
            bind.kind = BindInfo::BI_COORDINATE;
            bind.resolvedId = i;
            bind.type = TID_INT64;
            return bind;
        }
    }

    bind.type = TID_VOID;
    return bind;
}


BindInfo Expression::resolveContext(const boost::shared_ptr<AttributeReference>& ref,
                                    const boost::shared_ptr<Query>& query)
{
    BindInfo bind;

    // If array name presents point is used and it's not necessary to check variable usage
    if (ref->getArrayName() == "")
    {
        const string& varName = ref->getAttributeName();
        for (size_t i = 0; i < _variables.size(); i++)
        {
            if (_variables[i].name == varName) {
                return _bindings[i];
            }
        }
    }

    for (size_t a = 0; a < _inputSchemas.size(); a++)
    {
       bind = resolveContext(_inputSchemas[a], ref->getArrayName(), ref->getAttributeName(), query);
        if (bind.type != TID_VOID) {
            bind.inputNo = a;
            return bind;
        }
    }
    bind = resolveContext(_outputSchema, ref->getArrayName(), ref->getAttributeName(), query);
    if (bind.type != TID_VOID) {
        bind.inputNo = ~0;
        return bind;
    }

    throw USER_QUERY_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_REF_NOT_FOUND, ref->getParsingContext())
        << (ref->getArrayName() + "." + ref->getAttributeName());
}

void Expression::swapArguments(size_t firstIndex)
{
    // Search index of function that output result into current result index to change its output to input of converter
    size_t producer1 = 0;
    for (; producer1 < _functions.size(); producer1++)
    {
        if (_functions[producer1].resultIndex == firstIndex) {
            break;
        }
    }
    size_t producer2 = 0;
    for (; producer2 < _functions.size(); producer2++)
    {
        if (_functions[producer2].resultIndex == firstIndex + 1) {
            break;
        }
    }

    if (producer1 < _functions.size()) {
        _functions[producer1].resultIndex = firstIndex + 1;
    }
    if (producer2 < _functions.size()) {
        _functions[producer2].resultIndex = firstIndex;
    }

    // Search index of context that assign binding value to current result index to change its index to input of converter
    size_t contextIdx1 = 0;
    bool found1 = false;
    if (producer1 == _functions.size()) {
        for ( ; contextIdx1 < _contextNo.size(); contextIdx1++)
        {
            vector<size_t>& indicies = _contextNo[contextIdx1];
            for (producer1 = 0; producer1 < indicies.size(); producer1++) {
                if (indicies[producer1] == firstIndex) {
                    found1 = true;
                    break;
                }
            }
            if (found1)
                break;
        }
    }
    size_t contextIdx2 = 0;
    bool found2 = false;
    if (producer2 == _functions.size()) {
        for ( ; contextIdx2 < _contextNo.size(); contextIdx2++)
        {
            vector<size_t>& indicies = _contextNo[contextIdx2];
            for (producer2 = 0; producer2 < indicies.size(); producer2++) {
                if (indicies[producer2] == firstIndex + 1) {
                    found2 = true;
                    break;
                }
            }
            if (found2)
                break;
        }
    }

    if (found1) {
        _contextNo[contextIdx1][producer1] = firstIndex + 1;
    }
    if (found2) {
        _contextNo[contextIdx2][producer2] = firstIndex;
    }

    /**
     * Exchanging properties and values for indecies
     */
    ArgProp tmpProp = _props[firstIndex];
    _props[firstIndex] = _props[firstIndex + 1];
    _props[firstIndex + 1] = tmpProp;

    Value tmpValue = _eargs[firstIndex];
    _eargs[firstIndex] = _eargs[firstIndex + 1];
    _eargs[firstIndex + 1] = tmpValue;
}

const Expression::ArgProp&
Expression::internalCompile(boost::shared_ptr<LogicalExpression> expr,
                            const boost::shared_ptr<Query>& query,
                            bool tile,
                            size_t resultIndex, size_t skipIndex, bool skipValue)
{
    if (typeid(*expr) == typeid(AttributeReference))
    {
        boost::shared_ptr<AttributeReference> e = dynamic_pointer_cast<AttributeReference>(expr);
        const BindInfo bind = resolveContext(e, query);
        size_t i;
        for (i = 0; i < _bindings.size(); i++) {
            if (_bindings[i] == bind) {
                _contextNo[i].push_back(resultIndex);
                break;
            }
        }
        if (i == _bindings.size()) {
            _bindings.push_back(bind);
            _contextNo.push_back(vector<size_t>(1, resultIndex));
        }
        _props[resultIndex].type = bind.type;
    }
    else if (typeid(*expr) == typeid(Constant))
    {
        boost::shared_ptr<Constant> e = dynamic_pointer_cast<Constant>(expr);
        _props[resultIndex].type = e->getType();
        _eargs.resize(_props.size());
        _eargs[resultIndex] = tile ? makeTileConstant(e->getType(),e->getValue()) : e->getValue();
        _props[resultIndex].isConst = true;
        assert(_props[resultIndex].type != TID_VOID || e->getValue().isNull());
        _nullable |= e->getValue().isNull();
    }
    else if (typeid(*expr) == typeid(Function))
    {
        boost::shared_ptr<Function> e = dynamic_pointer_cast<Function>(expr);
        CompiledFunction f;
        f.functionName = e->getFunction();
        f.resultIndex = resultIndex;
        f.argIndex = _props.size();
        f.skipIndex = skipIndex;
        f.skipValue = skipValue;
        size_t functionIndex = _functions.size();
        _functions.push_back(f);
        // Reserving space for new values
        _props.resize(_props.size() + e->getArgs().size());

        // Compiling child expressions and saving their types
        vector<TypeId> funcArgTypes(e->getArgs().size());
        // Reverse order is needed to provide evaluation from left to right
        bool argumentsAreConst = true;
        for (int i = e->getArgs().size() - 1; i >=0 ; i--)
        {
            size_t newSkipIndex = skipIndex;
            bool newSkipValue = skipValue;
            if (!_tileMode) {
                if (i != 0 && !strcasecmp(f.functionName.c_str(), "iif")) {
                    if (i == 2) {
                        newSkipIndex = f.argIndex;
                        newSkipValue = true;
                    } else if (i == 1) {
                        newSkipIndex = f.argIndex;
                        newSkipValue = false;
                    }
                } else if (i == 1 && !strcasecmp(f.functionName.c_str(), "or")) {
                    newSkipIndex = f.argIndex;
                    newSkipValue = true;
                } else if (i == 1 && !strcasecmp(f.functionName.c_str(), "and")) {
                    newSkipIndex = f.argIndex;
                    newSkipValue = false;
                }
            }
            _props[f.argIndex + i] = internalCompile(e->getArgs()[i], query, tile,
                                                     f.argIndex + i,
                                                     newSkipIndex, newSkipValue);
            funcArgTypes[i] = _props[f.argIndex + i].type;
            argumentsAreConst = argumentsAreConst &&
                    (_props[f.argIndex + i].isConst || _props[f.argIndex + i].isConstantFunction);
        }
        // Searching function pointer for given function name and type of arguments
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        bool swapInputs = false;
        if (FunctionLibrary::getInstance()->findFunction(f.functionName, funcArgTypes,
                                                         functionDesc, converters, tile, swapInputs))
        {
            _functions[functionIndex].function = functionDesc.getFuncPtr();
            _functions[functionIndex].functionTypes = functionDesc.getInputArgs();
            if (functionDesc.getScratchSize() > 0)
                _functions[functionIndex].stateSize = functionDesc.getScratchSize();
            for (size_t i = 0; i < converters.size(); i++) {
                if (converters[i]) {
                    insertConverter(_functions[functionIndex].functionTypes[i],
                                    converters[i], f.argIndex + i, functionIndex, tile);
                }
            }
            _props[resultIndex].type = functionDesc.getOutputArg();
            if (swapInputs) {
                // We need to swap functions producing arguments for this call
                swapArguments(f.argIndex);
            }
            _props[resultIndex].isConstantFunction = functionDesc.isDeterministic() && argumentsAreConst;
            /**
             * TODO: here it would be useful to set isConst and notNull
                         * flags for result arg prop. Also here we can evaluate function
                         * if every its argument is constant and place in result index
                         * constant value with the result without adding CompiledFunction
                     * into _functionMap at all.
             */
        }
        else
        {
            stringstream ss;
            ss << f.functionName << '(';
            for (size_t i = 0; i < funcArgTypes.size(); i++) {
                ss << TypeLibrary::getType(funcArgTypes[i]).name();
                if (i < funcArgTypes.size() - 1)
                    ss << ", ";
            }
            ss << ')';
            throw USER_QUERY_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_FUNCTION_NOT_FOUND, e->getParsingContext())
                << ss.str();
        }
    }
    else
        assert(false);

    return _props[resultIndex];
}

void Expression::insertConverter(TypeId newType, FunctionPointer converter,
                                 size_t resultIndex, int64_t functionIndex, bool tile)
{
    // Search index of function that output result into current result index to change its output to input of converter
    size_t resultProducer;
    for (resultProducer = functionIndex < 0 ? 0 : functionIndex; resultProducer < _functions.size(); resultProducer++)
    {
        if (_functions[resultProducer].resultIndex == resultIndex) {
            break;
        }
    }
    if (resultProducer < _functions.size()) {
        /**
         * There is needed function and we change its resultIndex
         */
        CompiledFunction f;
        f.resultIndex = resultIndex;
        f.function = converter;
        f.argIndex = _props.size();
        f.skipIndex = _functions[resultProducer].skipIndex;
        f.skipValue = _functions[resultProducer].skipValue;
        f.functionTypes.push_back(_props[resultIndex].type);
        f.functionTypes.push_back(newType);
        _props.push_back(_props[resultIndex]);
        _props[resultIndex].type = newType;
        _functions[resultProducer].resultIndex = f.argIndex;
        _functions.insert(_functions.begin() + functionIndex + 1, f);
        return;
    }

    // Search index of context that assign binding value to current result index to change its index to input of converter
    size_t contextIdx;
    bool found = false;
    for (contextIdx = 0; contextIdx < _contextNo.size(); contextIdx++)
    {
        vector<size_t>& indicies = _contextNo[contextIdx];
        for (resultProducer = 0; resultProducer < indicies.size(); resultProducer++) {
            if (indicies[resultProducer] == resultIndex) {
                found = true;
                break;
            }
        }
        if (found)
            break;
    }
    if (found) {
        /**
         * There is needed context and we change its resultIndex
         */
        CompiledFunction f;
        f.resultIndex = resultIndex;
        f.function = converter;
        f.argIndex = _props.size();
        f.functionTypes.push_back(_props[resultIndex].type);
        f.functionTypes.push_back(newType);
        _props.push_back(_props[resultIndex]);
        _props[resultIndex].type = newType;
        _contextNo[contextIdx][resultProducer] = f.argIndex;
        _functions.insert(_functions.begin() + functionIndex + 1, f);
        return;
    }

    /**
     *  There is no function which output result into position to be converter,
     *  so convert in-place and only once now.
     */
    assert(_eargs[resultIndex].isNull() || _eargs[resultIndex].isTile() || _props[resultIndex].type   != TID_VOID);
    assert(_eargs[resultIndex].isNull() || _eargs[resultIndex].isTile() || _eargs[resultIndex].data() != NULL    );
    _props[resultIndex].type = newType;
    Type const& resType(TypeLibrary::getType(newType));
    Value val = tile ? Value(resType,Value::asTile) : Value(resType);
    const Value* v = &_eargs[resultIndex];
    converter(&v, &val, NULL);
    _eargs[resultIndex] = val;
}

void Expression::toString (std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false) << "[Expression] ";
    out << "resultType " <<  TypeLibrary::getType(_resultType).name();
    out << "\n";

    for (size_t i = 0; i < _inputSchemas.size(); i++ )
    {
        out << prefix(' ') << "[in] "<< _inputSchemas[i] << "\n";
    }
    out << prefix(' ') << "[out] "<< _outputSchema << "\n";
}

const Value& Expression::evaluate(ExpressionContext& e)
{
    assert(e._context.size() == _contextNo.size());
    const CompiledFunction * f = NULL;

    /**
     * Loop for every function to be performed due expression evaluating
     */
    try {
       for (size_t i = _functions.size(); i > 0; i--)
        {
            f = &_functions[i - 1];

         /* Can this evaluation potentially be skipped, perhaps because it
            occurs within argument position of a special form like IIF, AND,
            or OR...? */

            if (f->skipIndex)
            {
             /* Locate the  boolean value that determines whether or not we
                short circuit: for IIF, for example, this is the conditional
                expression in the first argument position...*/

                const Value* v = e._args[f->skipIndex];

            /* Compare 'v's actual value with 'skipValue', the value that the
               expression compiler determined earlier should lead to a short
               circuit. Notice here that when used as a conditonal exprerssion,
               a null value is currently treated as if it were 'false'. PB
               notes we may want to redefine this behaviour, however...*/

                if (f->skipValue == (v->isNull() ? false : v->getBool()))
                    continue;
            }

            f->function((const Value**)&e._args[f->argIndex], e._args[f->resultIndex], e._state[i-1].get());
        }
    } catch (const Exception& ex) {
        throw;
    } catch (const std::exception& ex) {
        throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_ERROR_IN_UDF)
            << ex.what() << f->functionName;
    } catch ( ... ) {
        throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_UNKNOWN_ERROR_IN_UDF)
            << f->functionName;
    }

    return *e._args[0];
}


void Expression::resolveFunctions()
{
    for (size_t i = 0; i < _functions.size(); i++)
    {
        CompiledFunction& f = _functions[i];
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        if (f.functionName.empty()) {
            // Converter case
            assert(f.functionTypes.size() == 2);
            f.function = FunctionLibrary::getInstance()->findConverter(f.functionTypes[0], f.functionTypes[1], _tileMode);
        }
        else {
            // Function case
            if (FunctionLibrary::getInstance()->findFunction(f.functionName, f.functionTypes,
                                                             functionDesc, converters, _tileMode))
            {
                f.function = functionDesc.getFuncPtr();
                if (functionDesc.getScratchSize() > 0)
                    f.stateSize = functionDesc.getScratchSize();
                assert(functionDesc.getOutputArg() == _props[f.resultIndex].type);
                assert(converters.size() == 0);
            }
            else
            {
                stringstream ss;
                ss << f.functionName << '(';
                for (size_t i = 0; i < f.functionTypes.size(); i++) {
                    ss << TypeLibrary::getType(f.functionTypes[i]).name();
                    if (i < f.functionTypes.size() - 1)
                        ss << ", ";
                }
                ss << ')';
                throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_FUNCTION_NOT_FOUND)
                    << ss.str();
            }
        }
    }
}

void Expression::clear()
{
    _bindings.clear();
    _compiled = false;
    _constant = false;
    _contextNo.clear();
    _eargs.resize(1);
    _functions.clear();
    _nullable = false;
    _props.resize(1);
}


// Functions
Value evaluate(boost::shared_ptr<LogicalExpression> expr,
               const boost::shared_ptr<Query>& query, TypeId expectedType)
{
    Expression e;
    e.compile(expr, query, false, expectedType);
    return e.evaluate();
}

TypeId expressionType(boost::shared_ptr<LogicalExpression> expr,
                      const boost::shared_ptr<Query>& query,
                      const vector< ArrayDesc>& inputSchemas)
{
    Expression e;
    e.compile(expr, query, false, TID_VOID, inputSchemas);
    return e.getType();
}

} // namespace

