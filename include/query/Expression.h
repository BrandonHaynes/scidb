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
 * @file Expression.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Expression evaluator that compiles logical expression and use inline Value to perform
 * fast evaluations.
 */

#ifndef EXPRESSION_H_
#define EXPRESSION_H_

#include <boost/serialization/vector.hpp>
#include <boost/shared_array.hpp>

#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "query/FunctionLibrary.h"
#include <query/Query.h>

namespace scidb
{
class LogicalExpression;
struct BindInfo
{
    enum {
        BI_ATTRIBUTE,
        BI_COORDINATE,
        BI_VALUE
    } kind;
    size_t inputNo; /**< ~0 - the output array value */
    size_t resolvedId;
    TypeId type;
    Value value;

    bool operator==(const BindInfo& bind)
    {
        return bind.kind == kind && bind.inputNo == inputNo && bind.resolvedId == resolvedId && bind.type == type;
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & (int&)kind;
        ar & inputNo;
        ar & resolvedId;
        ar & type;
        ar & value;
    }
};

struct VarInfo
{
    std::string name;
    TypeId type;
    VarInfo(const std::string& _name, const TypeId& _type):
        name(_name), type(_type)
    {
    }
};

/**
 * Use this class to hold context of Expression used for evaluations
 * It makes Expression evaluations reentrant.
 */
class ExpressionContext
{
friend class Expression;
private:
    /**
     * A vector of Value objects where context variables should go.
     * Pointers to these values can be used multiple times.
     */
    std::vector<Value> _context;
    class Expression& _expression;
    std::vector<Value*> _args; /**< Pointers to Value objects which will be used for evaluations. */
    std::vector<Value> _vargs; /**< Value objects which will be used for evaluations. */
    bool _contextChanged;
    std::vector< boost::shared_array<char> > _state;

public:
    ExpressionContext(Expression& expression);

    const Value& operator[](int i) const;
    Value& operator[](int i);
};

/**
 * Class Expression evaluates an expression using Value objects.
 * To do this it will use FunctionLibrary to find function with related types.
 * If there is no such functions Expression tries to insert converter which will be searched
 * in the FunctionLibrary too.
 */
class Expression
{
friend class ExpressionContext;
public:
    Expression(): _compiled(false), _tileMode(false),
        _tempValuesNumber(0), _eargs(1), _props(1)
    {
    }

    virtual ~Expression() {}

    /**
     * @param expr a pointer to logical expression tree to be compiled
     * @param inputs a vector of input arrays for resolving context references
     * @param expectedType a type of the expression result that is expected by user
     * @param query performing the compilation
     *
     * @return a type of result
     */
    void compile(boost::shared_ptr<LogicalExpression> expr,
                 const boost::shared_ptr<Query>& query,
                 bool tile,
                 TypeId expectedType =  TID_VOID,
                 const std::vector< ArrayDesc >& inputSchemas = std::vector< ArrayDesc >(),
                 const ArrayDesc& outputSchema = ArrayDesc());
    /**
     * @brief this method is useful for internal using in operators
     * to perform evaluations of binary functions.
     *
     * @param functionName a name of function that should be compiled
     * @param leftType a type id of left operand
     * @param rightType a type id of right operand
     * @param expectedType an expected type of result
     */
    void compile(std::string functionName,
             bool tile,
             TypeId leftType, TypeId rightType,
             TypeId expectedType = TID_VOID);

    /**
     * @brief this method is useful for internal using in operators
     * to perform evaluations of functions.
     *
     * @param expression a string with expression that should be compiled
     * @param names strings with names of variables which are used in expression
     * @param types a vector of variable types
     * @param expectedType an expected type of result
     */
    void compile(const std::string& expression, const std::vector<std::string>& names, const std::vector<TypeId>& types, TypeId expectedType = TID_VOID);

    /**
     * @brief this method is useful for internal using in operators
     * to prepare constant expression for example for
     * physical operator parameters.
     *
     * @param type a type id of expression
     * @param value an value of constant
     */
    void compile(bool tile, const TypeId& type, const Value& value);

    /**
     * @param context a vector of Value objects
     *
     * @return a constant reference to evaluated Value object
     */
    const Value& evaluate(ExpressionContext& e);

    /**
     * @return an evaluated Value object
     */
    Value evaluate() {
        ExpressionContext e(*this);
        return evaluate(e);
    }

    /**
     * @return a resulting type of expression
     */
     TypeId getType() const {
        return _resultType;
    }

    bool supportsTileMode() const {
        return _tileMode;
    }

    bool isNullable() const {
        return _nullable;
    }

    bool isConstant() const {
        return _props.size() > 0 && (_props[0].isConst || _props[0].isConstantFunction);
    }

    const std::vector<BindInfo>& getBindings() const {
        return _bindings;
    }

    void addVariableInfo(const std::string& name, const TypeId& type);

private:
    TypeId _resultType;
    std::vector< ArrayDesc > _inputSchemas;
    ArrayDesc _outputSchema;
    std::vector<BindInfo> _bindings;
    std::vector<VarInfo> _variables;
    std::vector<std::vector<size_t> > _contextNo; /**< A vector of vectors with resultIndex's where context variables should go */
    bool _compiled;
    bool _nullable;
    bool _constant; // doesn't depend on input data
    bool _tileMode;
    size_t _tempValuesNumber;

    /**
     * Structures to hold compiled expression
     */
    struct CompiledFunction
    {
        FunctionPointer function; /**< A pointer to a function */
        size_t argIndex; /**< An index to arguments of function */
        size_t resultIndex; /**< An index where to put result value */
        // TODO: next 2 fields should be replaced by FunctionDescriptor later for flexibility
        std::string functionName; /**< A name of function for debug and serialization purposes */
        std::vector< TypeId> functionTypes; /**< Function type ids for debug and serializetion purposes */
        bool skipValue;
        size_t skipIndex; /**< Index where to look value to skip evaluation if it's equal skipValue */
        size_t stateSize;

        CompiledFunction(): skipValue(false), skipIndex(0), stateSize(0) {
        }

        template<class Archive>
        void serialize(Archive& ar, const unsigned int version)
        {
            ar & argIndex;
            ar & resultIndex;
            ar & functionName;
            ar & functionTypes;
            ar & skipValue;
            ar & skipIndex;
        }
    };
    std::vector<CompiledFunction> _functions;

    std::vector<Value> _eargs;
    struct ArgProp
    {
        TypeId type;
        bool isConst;  /**< true if value presents and is constant */
        bool isConstantFunction;
        ArgProp(): type( TID_VOID), isConst(false), isConstantFunction(false)
        {
        }
        ArgProp& operator=(const ArgProp& val)
        {
            type = val.type;
            isConst = val.isConst;
            isConstantFunction = val.isConstantFunction;
            return *this;
        }

        template<class Archive>
        void serialize(Archive& ar, const unsigned int version)
        {
            ar & type;
            ar & isConst;
            ar & isConstantFunction;
        }
    };
    std::vector<ArgProp> _props; /**< a vector of argument properties for right compilation and optimizations */

    /**
     * The method resolves attribute or dimension reference
     *
     * @param arrayName a name of array
     * @param referenceName a name of attribute or dimension
     * @param query performing the operation
     * @return BindInfo structure with resolved IDs
     */
    BindInfo resolveContext(const boost::shared_ptr<AttributeReference>& ref,
                            const boost::shared_ptr<Query>& query);

    /**
     * The method resolves attribute or dimension reference for given ArrayDesc
     *
     * @param arrayDesc
     * @param arrayName a name of array
     * @param referenceName a name of attribute or dimension
     * @param query performing the operation
     * @return BindInfo structure with resolved IDs
     */
    BindInfo resolveContext(const ArrayDesc& arrayDesc, const std::string& arrayName,
                            const std::string& referenceName,
                            const boost::shared_ptr<Query>& query);

    /**
     * swap arguments to provide commulative function call
     * @param firstIndex an index of first agrument. It will be swapped with the next one
     */
    void swapArguments(size_t firstIndex);

    /**
     * Recursive function to compile expression
     * @param exp the logical expression to compile
     * @param query performing the operation
     */
    const ArgProp& internalCompile(boost::shared_ptr<LogicalExpression> expr,
                                   const boost::shared_ptr<Query>& query,
                                   bool tile,
                                   size_t resultIndex, size_t skipIndex, bool skipValue);

    /**
     * The function inserts given converter to given position
     *
     * @param functionPointer a pointer to converter to be inserted
     * @param resultIndex an index in _args vector where type converter must save a result
     * @oaram functionIndex an index of function after that converter must be inserted
     */
    void insertConverter(TypeId newType,
                         FunctionPointer converter,
                         size_t resultIndex,
                         int64_t functionIndex,
                         bool tile);

    /**
     * Resolves Function pointers after loading expression from stream.
     * Called from serialize method
     */
    void resolveFunctions();

    void clear();

public:

    /**
     * Serialize expression into boost::Archive for transfer to remote instances with
     * physical plan.
     */
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        const bool hadFunctions = _functions.size(); // True for loading

        ar & _resultType;
        ar & _bindings;
        ar & _contextNo;
        ar & _props;
        ar & _eargs;
        ar & _functions;
        ar & _compiled;
        ar & _tileMode;
        ar & _tempValuesNumber;

        if (!hadFunctions && _functions.size())
            resolveFunctions();
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
    * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString (std::ostream &out, int indent = 0) const;
};


/**
 * This function compile and evaluate a logical expression without context and
 * cast the result to expectedType.
 * This function is useful in LogicalOperators where expression is not compiled yet.
 *
 * @param expr a logical expression for evaluating
 * @param expectedType a type of result value if it's omitted the type will be inferred from expression
 * @param query performing the operation
 */
 Value evaluate(boost::shared_ptr<LogicalExpression> expr,
                const boost::shared_ptr<Query>& query,
                TypeId expectedType =  TID_VOID);


/**
 * This function compile and infer type of a logical expression and
 * cast the result to expectedType.
 * This function is useful in LogicalOperators where expression is not compiled yet.
 *
 * @param expr a logical expression for evaluating
 * @param expectedType a type of result value if it's omitted the type will be inferred from expression
 * @param query performing the operation
 */
 TypeId expressionType(boost::shared_ptr<LogicalExpression> expr,
                       const boost::shared_ptr<Query>& query,
                       const std::vector< ArrayDesc >& inputSchemas = std::vector< ArrayDesc >());

} // namespace

#endif
