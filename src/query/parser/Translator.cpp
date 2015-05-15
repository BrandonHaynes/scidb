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

/****************************************************************************/

#include <array/Compressor.h>
#include <query/ParsingContext.h>
#include <query/Serialize.h>
#include "AST.h"

/****************************************************************************/

#define PLACEHOLDER_OUTPUT_FLAG (PLACEHOLDER_END_OF_VARIES << 1)
#define QPROC(id,x)    USER_QUERY_EXCEPTION(SCIDB_SE_QPROC,   id,this->newParsingContext(x))
#define SYNTAX(id,x)   USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,  id,this->newParsingContext(x))
#define INTERNAL(id,x) USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL,id,this->newParsingContext(x))

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.altranslator"));

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

typedef shared_ptr<ParsingContext>              ContextPtr;
typedef shared_ptr<LogicalExpression>           LEPtr;
typedef shared_ptr<LogicalQueryPlanNode>        LQPNPtr;
typedef shared_ptr<OperatorParamArrayReference> OPARPtr;

/****************************************************************************/

class Translator
{
 public:
                              Translator(Factory& f,Log& l,const StringPtr& s,const QueryPtr& q=QueryPtr())
                                : _fac(f),_log(l),_txt(s),_qry(q)
                              {}

 public:
            LQPNPtr           AstToLogicalPlan          (const Node*,bool = false);
            LEPtr             AstToLogicalExpression    (const Node*);

 private:
            LQPNPtr           passAFLOperator           (const Node*);
            LQPNPtr           passSelectStatement       (const Node*);
            LQPNPtr           passJoins                 (const Node*);
            LQPNPtr           passIntoClause            (const Node*,LQPNPtr&);
            LQPNPtr           passGeneralizedJoin       (const Node*);
            LQPNPtr           passCrossJoin             (const Node*);
            LQPNPtr           passJoinItem              (const Node*);
            LQPNPtr           passImplicitScan          (const Node*);
            LQPNPtr           passFilterClause          (const Node*,const LQPNPtr&);
            LQPNPtr           passOrderByClause         (const Node*,const LQPNPtr&);
            LQPNPtr           passThinClause            (const Node*);
            LQPNPtr           passSelectList            (LQPNPtr&,const Node*,const Node*);
            LQPNPtr           passUpdateStatement       (const Node*);
            LQPNPtr           passInsertIntoStatement   (const Node*);

 private:
    shared_ptr<OperatorParamAggregateCall>
                              passAggregateCall               (const Node*,const vector<ArrayDesc>&);
            Value             passConstantExpression          (const Node*,const TypeId&);
            int64_t           passIntegralExpression          (const Node*);
            void              passSchema                      (const Node*,ArrayDesc &,const string&);
            void              passReference                   (const Node*,chars&,chars&);
            bool              passGeneralizedJoinOnClause     (vector<shared_ptr<OperatorParamReference> > &params,const Node*);
            void              passDimensions                  (const Node*,Dimensions&,const string&,set<string>&);

 private:
            LQPNPtr           fitInput                        (LQPNPtr&,const ArrayDesc&);
            LQPNPtr           canonicalizeTypes               (const LQPNPtr&);
            LQPNPtr           appendOperator                  (const LQPNPtr&,const string&,const LogicalOperator::Parameters&,const ContextPtr&);
            string            placeholdersToString            (const vector<shared_ptr<OperatorParamPlaceholder> > &)const;
            string            astParamToString                (const Node*)const;
            bool              resolveParamAttributeReference  (const vector<ArrayDesc>&,shared_ptr<OperatorParamReference> &,bool = true);
            bool              resolveParamDimensionReference  (const vector<ArrayDesc>&,shared_ptr<OperatorParamReference> &,bool = true);
            bool              placeholdersVectorContainType   (const vector<shared_ptr<OperatorParamPlaceholder> > &,OperatorParamPlaceholderType );
            bool              checkAttribute                  (const vector<ArrayDesc>&,const string& aliasName,const string& attributeName,const ContextPtr&);
            bool              checkDimension                  (const vector<ArrayDesc>&,const string& aliasName,const string& dimensionName,const ContextPtr&);
            void              checkLogicalExpression          (const vector<ArrayDesc>&,const ArrayDesc &,const LEPtr &);
            string            genUniqueObjectName             (const string&,unsigned int &,const vector<ArrayDesc> &,bool,cnodes  = cnodes());
            void              prohibitDdl                     (const LQPNPtr&);
            void              prohibitNesting                 (const LQPNPtr&);
            Node*             decomposeExpression             (const Node*,vector<Node*>&,vector<Node*>&,unsigned&,bool,const ArrayDesc&,const set<string>&,bool,bool&);
            int64_t           estimateChunkInterval           (cnodes);
            OPARPtr           createArrayReferenceParam       (const Node*,bool);
            bool              resolveDimension                (const vector<ArrayDesc> &inputSchemas,const string&,const string&,size_t&,size_t&,const ContextPtr&,bool);
            bool              astHasUngroupedReferences       (const Node*,const set<string>&)const;
            bool              astHasAggregates                (const Node*)                   const;
            bool              matchOperatorParam              (const Node*,const OperatorParamPlaceholders &,vector<ArrayDesc> &inputSchemas,vector<LQPNPtr> &inputs,shared_ptr<OperatorParam> &param);

 private:                  // Expressions
            LEPtr             onNull              (const Node*);
            LEPtr             onReal              (const Node*);
            LEPtr             onInteger           (const Node*);
            LEPtr             onBoolean           (const Node*);
            LEPtr             onString            (const Node*);
            LEPtr             onScalarFunction    (const Node*);
            LEPtr             onAttributeReference(const Node*);

 private:
            ContextPtr        newParsingContext(const Node*  n)                         {return make_shared<ParsingContext>(_txt,n->getWhere());}
            ContextPtr        newParsingContext(const shared_ptr<OperatorParam>&  n)    {return n->getParsingContext();}
            ContextPtr        newParsingContext(const shared_ptr<AttributeReference>& n){return n->getParsingContext();}
            ContextPtr        newParsingContext(const ContextPtr& n)                    {return n;}
            void              fail(const UserException& x)                              {_log.fail(x);}

 private:                  // Representation
            Factory&          _fac;                      // Abstract factory
            Log&              _log;                      // Abstract error log
      const StringPtr&        _txt;                      // The source text: yuk!
            QueryPtr    const _qry;                      // The query
};

/****************************************************************************/

/**
 *  If the node 'ast' has child 'c' return the value of this string. Otherwise
 *  return the given default string.
 */
static chars getString(const Node* ast,child c,chars otherwise = "")
{
    assert(ast!=0 && otherwise!=0);                      // Validate arguments

    if (const Node* p = ast->get(c))                     // Has child 'c'?
    {
        return p->getString();                           // ...return value
    }

    return otherwise;                                    // Return the default
}

/**
 *  If the node 'ast' has child 'c' return the value of this boolean. Otherwise
 *  return the given default value.
 */
static boolean getBoolean(const Node* ast,child c,boolean otherwise = false)
{
    assert(ast != 0);                                    // Validate arguments

    if (const Node* p = ast->get(c))                     // Has child 'c'?
    {
        return p->getBoolean();                          // ...return value
    }

    return otherwise;                                    // Return the default
}

/**
 *  If the node 'ast' has child 'c' return the value of this integer. Otherwise
 *  return the given default value.
 */
static integer getInteger(const Node* ast,child c,integer otherwise = 0)
{
    assert(ast != 0);                                    // Validate arguments

    if (const Node* p = ast->get(c))                     // Has child 'c'?
    {
        return p->getInteger();                          // ...return value
    }

    return otherwise;                                    // Return the default
}

const Node* getChildSafely(const Node* ast,child c)
{
    assert(ast != 0);                                    // Validate arguments

    if (const Node* p = ast->get(c))                     // Has child 'c'?
    {
        return p;                                        // ...so return it
    }

    return ast;                                          // Go with the parent
}

/****************************************************************************/
// temporary glue while introducing type 'variable' into the AST hiererchy
static Name* getApplicationArgName         (const Node* n){assert(n!=0 && n->is(application));return n->get(applicationArgOperator)->get(variableArgName);}
static Name* getReferenceArgName           (const Node* n){assert(n!=0 && n->is(reference  ));return n->get(referenceArgName)      ->get(variableArgName);}
static Name* getReferenceArgArrayName      (const Node* n){assert(n!=0 && n->is(reference  ));Node*  N = n->get(referenceArgArray);return N?N->get(variableArgName):0;}
static chars getStringApplicationArgName   (const Node* n){if (const Node* s=getApplicationArgName(n))   {return s->getString();} return "";}
static chars getStringReferenceArgName     (const Node* n){if (const Node* s=getReferenceArgName(n))     {return s->getString();} return "";}
static chars getStringReferenceArgArrayName(const Node* n){if (const Node* s=getReferenceArgArrayName(n)){return s->getString();} return "";}
/****************************************************************************/

LQPNPtr Translator::AstToLogicalPlan(const Node* ast,bool canonicalize)
{
    LQPNPtr r;

    switch (ast->getType())
    {
        default:          SCIDB_UNREACHABLE();  // jab:
        case application: r = passAFLOperator(ast);          break;
        case reference:   r = passImplicitScan(ast);         break;
        case insertArray: r = passInsertIntoStatement(ast);  break;
        case selectArray: r = passSelectStatement(ast);      break;
        case updateArray: r = passUpdateStatement(ast);      break;
    }

    if (canonicalize && !r->isDdl())
    {
        r = canonicalizeTypes(r);
    }

    return r;
}

int64_t Translator::estimateChunkInterval(cnodes nodes)
{
    int64_t const targetChunkSize    = 1000000;          // See #3936
    int64_t       knownChunksSize    = 1;
    size_t        unknownChunksCount = 0;

    BOOST_FOREACH (const Node* d,nodes)
    {
        assert(d->is(dimension));

        if (const Node* n = d->get(dimensionArgChunkInterval))
        {
            knownChunksSize *= passIntegralExpression(n);
        }
        else
        {
            ++unknownChunksCount;
        }
    }


 /* If all the dimensions were specified, then there is no need to guess...*/

    if (unknownChunksCount == 0)
    {
        return knownChunksSize;
    }

    int64_t r = floor(pow(max(1L,targetChunkSize/knownChunksSize),1.0/unknownChunksCount));
    assert(r > 0);
    return r;
}

Value Translator::passConstantExpression(const Node* ast,const TypeId& targetType)
{
    Expression pExpr;
    try
    {
        pExpr.compile(AstToLogicalExpression(ast),_qry,false,targetType);
    }
    catch (const Exception& e)
    {
        //Rewrite possible type exceptions to get error location in query
        if (SCIDB_SE_TYPE == e.getShortErrorCode())
        {
            fail(SYNTAX(SCIDB_LE_TYPE_EXPECTED,ast) << targetType);
        }
        throw;
    }

    if (!pExpr.isConstant())
    {
        fail(SYNTAX(SCIDB_LE_CONSTANT_EXPRESSION_EXPECTED,ast));
    }

    return pExpr.evaluate();
}

int64_t Translator::passIntegralExpression(const Node* ast)
{
    return passConstantExpression(ast,TID_INT64).getInt64();
}

void Translator::passDimensions(const Node* ast,Dimensions& dimensions,const string& arrayName,set<string> &usedNames)
{
    dimensions.reserve(ast->getSize());                  // Reserve memory

    BOOST_FOREACH (const Node* d,ast->getList())         // For each dimension
    {
        assert(d->is(dimension));                        // ...is a dimension

        string  const nm = d->get(dimensionArgName)->getString();
        int64_t       lo = 0;                            // ...lower bound
        int64_t       hi = INFINITE_LENGTH;              // ...upper bound
        int64_t       ci = 0;                            // ...chunk interval
        int64_t       co = 0;                            // ...chunk overlap

        if (!usedNames.insert(nm).second)                // ...already used?
        {
            fail(SYNTAX(SCIDB_LE_DUPLICATE_DIMENSION_NAME,d->get(dimensionArgName)) << nm);
        }

        if (const Node* n = d->get(dimensionArgLoBound))
        {
            lo = passIntegralExpression(n);
        }

        if (const Node* n = d->get(dimensionArgHiBound))
        if (!n->is(asterisk))
        {
            hi = passIntegralExpression(n);
        }

        if (const Node* n = d->get(dimensionArgChunkInterval))
        {
            ci = passIntegralExpression(n);
        }
        else
        {
            ci = estimateChunkInterval(ast->getList());
        }

        if (const Node* n = d->get(dimensionArgChunkOverlap))
        {
            co = passIntegralExpression(n);
        }

        if (lo == MAX_COORDINATE)
        {
            fail(SYNTAX(SCIDB_LE_DIMENSION_START_CANT_BE_UNBOUNDED,getChildSafely(d,dimensionArgLoBound)));
        }

        if (lo<=MIN_COORDINATE || MAX_COORDINATE<lo)
        {
            fail(SYNTAX(SCIDB_LE_INCORRECT_DIMENSION_BOUNDARY,getChildSafely(d,dimensionArgLoBound)) << MIN_COORDINATE << MAX_COORDINATE);
        }

        if (hi<=MIN_COORDINATE || MAX_COORDINATE<hi)
        {
            fail(SYNTAX(SCIDB_LE_INCORRECT_DIMENSION_BOUNDARY,getChildSafely(d,dimensionArgHiBound))<< MIN_COORDINATE << MAX_COORDINATE);
        }

        if (hi<lo && hi+1!=lo)
        {
            fail(SYNTAX(SCIDB_LE_HIGH_SHOULDNT_BE_LESS_LOW,getChildSafely(d,dimensionArgHiBound)));
        }

        if (ci <= 0)
        {
            fail(SYNTAX(SCIDB_LE_INCORRECT_CHUNK_SIZE,getChildSafely(d,dimensionArgChunkInterval)) << numeric_limits<int64_t>::max());
        }

        if (co < 0)
        {
            fail(SYNTAX(SCIDB_LE_INCORRECT_OVERLAP_SIZE,getChildSafely(d,dimensionArgChunkOverlap)) << numeric_limits<int64_t>::max());
        }

        if (co > ci)
        {
            fail(SYNTAX(SCIDB_LE_OVERLAP_CANT_BE_LARGER_CHUNK,getChildSafely(d,dimensionArgChunkOverlap)));
        }

        dimensions.push_back(DimensionDesc(nm,lo,hi,ci,co));
    }
}

void Translator::passSchema(const Node* ast,ArrayDesc& schema,const string& arrayName)
{
    const Node* list    = ast->get(schemaArgAttributes);
    Attributes attributes;
    attributes.reserve(list->getSize());
    set<string> usedNames;

    BOOST_FOREACH(const Node* attNode,list->getList())
    {
        assert(attNode->is(attribute));

        const string attName            = getString (attNode,attributeArgName);
        const string attTypeName        = getString (attNode,attributeArgTypeName);
        const bool   attTypeNullable    = getBoolean(attNode,attributeArgIsNullable);
        const string attCompressorName  = getString (attNode,attributeArgCompressorName,"no compression");

        Value defaultValue;

        if (usedNames.find(attName) != usedNames.end())
        {
            fail(SYNTAX(SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME,attNode->get(attributeArgName)) << attName);
        }

        usedNames.insert(attName);

        int16_t const attFlags = attTypeNullable ? AttributeDesc::IS_NULLABLE : 0;

        try
        {
            const Type attType(TypeLibrary::getType(attTypeName));

            if (attType == TypeLibrary::getType(TID_INDICATOR))
            {
                fail(SYNTAX(SCIDB_LE_EXPLICIT_EMPTY_FLAG_NOT_ALLOWED,attNode->get(attributeArgTypeName)));
            }

            string serializedDefaultValueExpr;

            if (const Node* defaultValueNode = attNode->get(attributeArgDefaultValue))
            {
                if (astHasUngroupedReferences(defaultValueNode, set<string>()))
                {
                    fail(SYNTAX(SCIDB_LE_REFERENCE_NOT_ALLOWED_IN_DEFAULT,defaultValueNode));
                }

                Expression e;
                e.compile(AstToLogicalExpression(defaultValueNode), _qry, false, attTypeName);
                serializedDefaultValueExpr = serializePhysicalExpression(e);
                defaultValue = e.evaluate();

                if (defaultValue.isNull() && !attTypeNullable)
                {
                    fail(SYNTAX(SCIDB_LE_NULL_IN_NON_NULLABLE,attNode->get(attributeArgName)) << attName);
                }
            }
            else
            {
                defaultValue = Value(attType);

                if (attTypeNullable)
                {
                    defaultValue.setNull();
                }
                else
                {
                    defaultValue = TypeLibrary::getDefaultValue(attType.typeId());
                }
            }

            const Compressor *attCompressor = NULL;
            BOOST_FOREACH (Compressor* c,CompressorFactory::getInstance().getCompressors())
            {
                if (c->getName() == attCompressorName)
                {
                    attCompressor = c;
                    break;
                }
            }

            if (attCompressor == NULL)
            {
                fail(SYNTAX(SCIDB_LE_COMPRESSOR_DOESNT_EXIST,attNode->get(attributeArgCompressorName)) << attCompressorName);
            }

            attributes.push_back(AttributeDesc(
                attributes.size(),
                attName,
                attType.typeId(),
                attFlags,
                attCompressor->getType(),
                set<string>(),
                getInteger(attNode,attributeArgReserve,Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE)),
                &defaultValue,
                serializedDefaultValueExpr));
        }
        catch (SystemException& e)
        {
            if (e.getLongErrorCode() == SCIDB_LE_TYPE_NOT_REGISTERED)
            {
                fail(CONV_TO_USER_QUERY_EXCEPTION(e, newParsingContext(attNode->get(attributeArgTypeName))));
            }

            throw;
        }
    }

    // In 14.3, all arrays became emptyable
    //FIXME: Which compressor for empty indicator attribute?
    attributes.push_back(AttributeDesc(attributes.size(), DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));

    Dimensions dimensions;

    passDimensions(ast->get(schemaArgDimensions),dimensions,arrayName,usedNames);

    schema = ArrayDesc(0,0,0,arrayName,attributes,dimensions);
}

LQPNPtr Translator::passAFLOperator(const Node *ast)
{
    const chars opName   = getStringApplicationArgName(ast);
    cnodes astParameters = ast->getList(applicationArgOperands);
    const string opAlias = getString(ast,applicationArgAlias);

    vector<LQPNPtr >            opInputs;
    vector<ArrayDesc>           inputSchemas;
    shared_ptr<LogicalOperator> op;

    try
    {
        op = OperatorLibrary::getInstance()->createLogicalOperator(opName,opAlias);
    }
    catch (Exception &e)
    {
        if (e.getLongErrorCode() == SCIDB_LE_LOGICAL_OP_DOESNT_EXIST)
        {
            fail(CONV_TO_USER_QUERY_EXCEPTION(e,newParsingContext(ast)));
        }

        throw;
    }

    const OperatorParamPlaceholders& opPlaceholders = op->getParamPlaceholders();

    // If operator not expecting any parameters
    if (opPlaceholders.empty())
    {
        //but AST has some, then throw syntax error, else just skip parameters parsing
        if (!astParameters.empty())
        {
            fail(SYNTAX(SCIDB_LE_UNEXPECTED_OPERATOR_ARGUMENT,ast->get(applicationArgOperands)) << opName << astParameters.size());
        }
    }
    else
    {
        //If operator parameters are variable, we don't know exact needed parameters count
        //If not check AST's parameters count and placeholders count
        const bool hasVaryParams = opPlaceholders.back()->getPlaceholderType() == PLACEHOLDER_VARIES;

        if (!hasVaryParams)
        {
            if (astParameters.size() != opPlaceholders.size())
            {
                fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT,ast->get(applicationArgOperands)) << opName <<  opPlaceholders.size() << astParameters.size());
            }
        }

        OperatorParamPlaceholders supposedPlaceholders;
        //Iterate over all parameters of operator and match placeholders
        size_t astParamNo = 0;
        while (true)
        {
            //Check if we need next iteration in case fixed arguments
            if (!hasVaryParams && astParamNo >= astParameters.size())
                break;

            //First we iterating over all fixed parameters, and then over all vary parameters
            if (hasVaryParams && astParamNo >= opPlaceholders.size() - 1)
            {
                supposedPlaceholders = op->nextVaryParamPlaceholder(inputSchemas);
            }
            else
            {
                supposedPlaceholders.clear();
                supposedPlaceholders.push_back(opPlaceholders[astParamNo]);
            }

            //Now check if we need to stop parsing vary arguments
            if (astParamNo >= astParameters.size())
            {
                //Here we don't have any arguments in AST and have placeholder indicated about arguments
                //end. Stopping parsing.
                if (placeholdersVectorContainType(supposedPlaceholders, PLACEHOLDER_END_OF_VARIES))
                {
                    break;
                }
                //And here we actually expected more arguments. Throwing error.
                else
                {
                    fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2,ast->get(applicationArgOperands)) << opName);
                }
            }
            else
            {
                if (placeholdersVectorContainType(supposedPlaceholders, PLACEHOLDER_END_OF_VARIES) && supposedPlaceholders.size() == 1)
                {
                    fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3,astParameters[astParamNo]) << opName << astParamNo);
                }
            }

            Node *astParam = astParameters[astParamNo];

            try
            {
                shared_ptr<OperatorParam> opParam;
                if (matchOperatorParam(astParam, supposedPlaceholders, inputSchemas, opInputs, opParam))
                    op->addParameter(opParam);
            }
            catch (const UserQueryException &e)
            {
                if (e.getShortErrorCode() == SCIDB_SE_INTERNAL && e.getLongErrorCode() == SCIDB_LE_WRONG_OPERATOR_ARGUMENT)
                {
                    fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENT,astParam)
                            << placeholdersToString(supposedPlaceholders)
                            << (astParamNo + 1)
                            << opName
                            << astParamToString(astParam));
                }
                throw;
            }

            ++astParamNo;
        }
    }

    if (opInputs.size() && op->getProperties().ddl)
    {
        fail(INTERNAL(SCIDB_LE_DDL_SHOULDNT_HAVE_INPUTS,ast));
    }

    LQPNPtr result = make_shared<LogicalQueryPlanNode>(newParsingContext(ast), op, opInputs);

    // We can't check expression before getting all operator parameters. So here we already have
    // all params and can get operator output schema. On each iteration we checking references in
    // all non-constant expressions. If ok, we trying to compile expression to check type compatibility.
    size_t paramNo = inputSchemas.size(); // Inputs parameters too, but only in AST
    BOOST_FOREACH(const shared_ptr<OperatorParam> &param, result->getLogicalOperator()->getParameters())
    {
        ++paramNo;
        if (PARAM_LOGICAL_EXPRESSION == param->getParamType())
        {
            const shared_ptr<OperatorParamLogicalExpression>& paramLE = (const shared_ptr<OperatorParamLogicalExpression>&) param;

            if (paramLE->isConstant())
                continue;

            const ArrayDesc& outputSchema = result->inferTypes(_qry);

            const LEPtr& lExpr = paramLE->getExpression();
            checkLogicalExpression(inputSchemas, outputSchema, lExpr);

            shared_ptr<Expression> pExpr = make_shared<Expression>();

            try
            {
               pExpr->compile(lExpr, _qry, false, paramLE->getExpectedType().typeId(), inputSchemas, outputSchema);
            }
            catch (const Exception &e)
            {
                if (e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR)
                {
                    fail(SYNTAX(SCIDB_LE_PARAMETER_TYPE_ERROR,param) << paramLE->getExpectedType().name() << pExpr->getType());
                }
                throw;
            }
        }
    }

    return result;
}

shared_ptr<OperatorParamArrayReference> Translator::createArrayReferenceParam(const Node *arrayReferenceAST,bool inputSchema)
{
    ArrayDesc schema;
    string arrayName = getStringReferenceArgName(arrayReferenceAST);
    string dimName;
    assert(arrayName != "");
    assert(arrayName.find('@') == string::npos);

    if (const Node* n = getReferenceArgArrayName(arrayReferenceAST))
    {
        fail(SYNTAX(SCIDB_LE_NESTED_ARRAYS_NOT_SUPPORTED,n));
    }

    if (!inputSchema)
    {
        assert(!arrayReferenceAST->get(referenceArgVersion));
        return make_shared<OperatorParamArrayReference>(newParsingContext(arrayReferenceAST),"",arrayName,inputSchema,0);
    }

    SystemCatalog *systemCatalog = SystemCatalog::getInstance();
    VersionID version = 0;

    if (!systemCatalog->getArrayDesc(arrayName, schema, false))
    {
        fail(QPROC(SCIDB_LE_ARRAY_DOESNT_EXIST,getReferenceArgName(arrayReferenceAST)) << arrayName);
    }

    version = LAST_VERSION;

    if (arrayReferenceAST->get(referenceArgVersion))
    {
        if (arrayReferenceAST->get(referenceArgVersion)->is(asterisk))
        {
            return make_shared<OperatorParamArrayReference>(newParsingContext(arrayReferenceAST), "",arrayName, inputSchema, ALL_VERSIONS);
        }
        else
        {
            LEPtr lExpr = AstToLogicalExpression(arrayReferenceAST->get(referenceArgVersion));
            Expression pExpr;
            pExpr.compile(lExpr, _qry, false);
            const Value &value = pExpr.evaluate();

            if (pExpr.getType() == TID_INT64)
            {
                version = value.getUint64();
                if (version > systemCatalog->getLastVersion(schema.getId()))
                {
                    version = 0;
                }
            }
            else if (pExpr.getType() == TID_DATETIME)
            {
                version = systemCatalog->lookupVersionByTimestamp(schema.getId(), value.getDateTime());
            }
            else
            {
                SCIDB_UNREACHABLE();
            }
        }
    }

    if (!version)
        fail(QPROC(SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST,arrayReferenceAST->get(referenceArgVersion)) << arrayName);
    systemCatalog->getArrayDesc(arrayName, version, schema);

    assert(arrayName.find('@') == string::npos);
    return make_shared<OperatorParamArrayReference>(newParsingContext(arrayReferenceAST), "",arrayName, inputSchema, version);
}

bool Translator::matchOperatorParam(const Node* ast,
        const OperatorParamPlaceholders &placeholders,
        vector<ArrayDesc> &inputSchemas,
        vector<LQPNPtr > &inputs,
        shared_ptr<OperatorParam> &param)
{
    int matched = 0;

    //Each operator parameter from AST can match several placeholders. We trying to catch best one.
    BOOST_FOREACH(const shared_ptr<OperatorParamPlaceholder>& placeholder, placeholders)
    {
        switch (placeholder->getPlaceholderType())
        {
            case PLACEHOLDER_INPUT:
            {
                LQPNPtr input;
                //This input is implicit scan.
                if (ast->is(reference))
                {
                    if (ast->has(referenceArgOrder))
                    {
                        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
                    }

                    input = passImplicitScan(ast);
                }
                //This input is result of other operator, so go deeper in tree and translate this operator.
                else if (ast->is(application) || ast->is(selectArray))
                {
                    input = AstToLogicalPlan(ast);
                    prohibitDdl(input);
                    prohibitNesting(input);
                }
                else
                {
                    break;
                }

                inputSchemas.push_back(input->inferTypes(_qry));
                inputs.push_back(input);

                //Inputs can not be mixed in vary parameters. Return and go to next parameter.
                return false;
            }

            case PLACEHOLDER_ARRAY_NAME:
            {
                if (ast->is(reference))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    if (ast->has(referenceArgVersion))
                    {
                        if (!(placeholder->getFlags() & PLACEHOLDER_ARRAY_NAME_VERSION))
                            fail(SYNTAX(SCIDB_LE_CANT_ACCESS_ARRAY_VERSION,ast->get(referenceArgVersion)));
                    }

                    if (ast->has(referenceArgOrder))
                    {
                        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
                    }

                    param = createArrayReferenceParam(ast, placeholder->isInputSchema());

                    matched |= PLACEHOLDER_ARRAY_NAME;
                }

                break;
            }

            case PLACEHOLDER_ATTRIBUTE_NAME:
            {
                if (ast->is(reference) && !ast->has(referenceArgVersion))
                {
                    shared_ptr<OperatorParamAttributeReference> opParam = make_shared<OperatorParamAttributeReference>(
                            newParsingContext(ast),
                            getStringReferenceArgArrayName(ast),
                            getStringReferenceArgName(ast),
                            placeholder->isInputSchema());

                    opParam->setSortAscent(getInteger(ast,referenceArgOrder,ascending) == ascending);

                    //Trying resolve attribute in input schema
                    if (placeholder->isInputSchema())
                    {
                        if (!resolveParamAttributeReference(inputSchemas, (shared_ptr<OperatorParamReference>&)opParam, false))
                            break;
                    }

                    //Check if something already matched in overloaded parameter
                    if (matched)
                    {
                        //If current parameter from input schema and some previous matched was from
                        //input schema, or current parameter from output schema and some previous
                        //matched was from output schema, so we can't resolve such ambigouty
                        if ((placeholder->isInputSchema() && !(matched & PLACEHOLDER_OUTPUT_FLAG))
                        || (!placeholder->isInputSchema() &&  (matched & PLACEHOLDER_OUTPUT_FLAG)))
                        {
                            fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                        }

                        //If some matched, but in different schema, prefer input schema parameter over
                        //output schema
                        if (placeholder->isInputSchema())
                        {
                            param = opParam;
                        }
                    }
                    else
                    {
                        param = opParam;
                    }

                    //Raise flags in any case, even parameter was not catched
                    matched |= PLACEHOLDER_ATTRIBUTE_NAME;
                    matched |= placeholder->isInputSchema() ? 0 : PLACEHOLDER_OUTPUT_FLAG;
                }
                break;
            }

            case PLACEHOLDER_DIMENSION_NAME:
            {
                if (ast->is(reference) && !ast->has(referenceArgVersion))
                {
                    shared_ptr<OperatorParamReference> opParam = make_shared<OperatorParamDimensionReference>(
                            newParsingContext(ast),
                            getStringReferenceArgArrayName(ast),
                            getStringReferenceArgName(ast),
                            placeholder->isInputSchema());

                    //Trying resolve dimension in input schema
                    if (placeholder->isInputSchema())
                    {
                        if (!resolveParamDimensionReference(inputSchemas, opParam, false))
                            break;
                    }

                    //Check if something already matched in overloaded parameter
                    if (matched)
                    {
                        //If current parameter from input schema and some previous matched was from
                        //input schema, or current parameter from output schema and some previous
                        //matched was from output schema, so we can't resolve such ambigouty
                        if ((placeholder->isInputSchema() && !(matched & PLACEHOLDER_OUTPUT_FLAG))
                             || (!placeholder->isInputSchema() && (matched & PLACEHOLDER_OUTPUT_FLAG)))
                        {
                            fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                        }

                        //If some matched, but in different schema, prefer input schema parameter over
                        //output schema
                        if (placeholder->isInputSchema())
                        {
                            param = opParam;
                        }
                    }
                    else
                    {
                        param = opParam;
                    }

                    //Raise flags in any case, even parameter was not catched
                    matched |= PLACEHOLDER_DIMENSION_NAME;
                    matched |= placeholder->isInputSchema() ? 0 : PLACEHOLDER_OUTPUT_FLAG;
                }
                break;
            }

            case PLACEHOLDER_CONSTANT:
            {
                if (ast->is(application)
                 || ast->is(cnull)
                 || ast->is(creal)
                 || ast->is(cstring)
                 || ast->is(cboolean)
                 || ast->is(cinteger))
                {
                    LEPtr lExpr;
                    shared_ptr<Expression> pExpr = make_shared<Expression>();

                    try
                    {
                       lExpr = AstToLogicalExpression(ast);
                       pExpr->compile(lExpr, _qry, false, placeholder->getRequiredType().typeId());
                    }
                    catch (const Exception &e)
                    {
                        if (e.getLongErrorCode() == SCIDB_LE_REF_NOT_FOUND
                            || e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR
                            || e.getLongErrorCode() == SCIDB_LE_UNEXPECTED_OPERATOR_IN_EXPRESSION)
                        {
                            break;
                        }
                    }

                    //Ignore non-constant expressions to avoid conflicts with
                    //PLACEHOLDER_EXPRESSION and PLACEHOLDER_AGGREGATE_CALL
                    if (!pExpr->isConstant())
                    {
                        break;
                    }

                    if (matched && !(matched & PLACEHOLDER_CONSTANT))
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    if (!(matched & PLACEHOLDER_CONSTANT))
                    {
                        param = make_shared<OperatorParamLogicalExpression>(newParsingContext(ast),
                                    lExpr, placeholder->getRequiredType(), true);
                    }
                    else
                    {
                        pExpr->compile(lExpr, _qry, false);

                        if (pExpr->getType() == placeholder->getRequiredType().typeId())
                        {
                            param = make_shared<OperatorParamLogicalExpression>(newParsingContext(ast),
                                        lExpr, placeholder->getRequiredType(), true);
                        }
                    }

                    matched |= PLACEHOLDER_CONSTANT;
                }
                break;
            }

            case PLACEHOLDER_EXPRESSION:
            {
                if (ast->is(application)
                 || ast->is(reference)
                 || ast->is(cnull)
                 || ast->is(creal)
                 || ast->is(cstring)
                 || ast->is(cboolean)
                 || ast->is(cinteger))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    LEPtr lExpr = AstToLogicalExpression(ast);

                    //We not checking expression now, because we can't get output schema. Checking
                    //will be done after getting all operator parameters
                    param = make_shared<OperatorParamLogicalExpression>(newParsingContext(ast),
                            lExpr, placeholder->getRequiredType(), false);

                    matched |= PLACEHOLDER_EXPRESSION;
                }

                break;
            }

            case PLACEHOLDER_SCHEMA:
            {
                if (ast->is(schema))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    ArrayDesc schema;

                    passSchema(ast, schema, "");

                    param = make_shared<OperatorParamSchema>(newParsingContext(ast), schema);

                    matched |= PLACEHOLDER_SCHEMA;
                }
                else
                if (ast->is(reference))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    if (getReferenceArgArrayName(ast) != 0)
                    {
                        fail(SYNTAX(SCIDB_LE_NESTED_ARRAYS_NOT_SUPPORTED,ast));
                    }

                    const chars arrayName = getStringReferenceArgName(ast);
                    ArrayDesc schema;
                    if (!SystemCatalog::getInstance()->getArrayDesc(arrayName, schema, false))
                    {
                        fail(SYNTAX(SCIDB_LE_ARRAY_DOESNT_EXIST, ast) << arrayName);
                    }

                    param = make_shared<OperatorParamSchema>(newParsingContext(ast), schema);

                    matched |= PLACEHOLDER_SCHEMA;
                }
                break;
            }

            case PLACEHOLDER_AGGREGATE_CALL:
            {
                if (ast->is(application))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    param = passAggregateCall(ast, inputSchemas);
                    matched |= PLACEHOLDER_AGGREGATE_CALL;
                }
                break;
            }

            case PLACEHOLDER_END_OF_VARIES:
                break;

            default:    SCIDB_UNREACHABLE();
        }
    }

    if (!matched)
    {
        fail(QPROC(SCIDB_LE_WRONG_OPERATOR_ARGUMENT2,ast) << placeholdersToString(placeholders));
    }

    return true;
}

string Translator::placeholdersToString(const vector<shared_ptr<OperatorParamPlaceholder> > & placeholders) const
{
    bool first = true;
    ostringstream ss;
    BOOST_FOREACH(const shared_ptr<OperatorParamPlaceholder> &placeholder, placeholders)
    {
        if (!first)
            ss << " or ";
        first = false;
        switch (placeholder->getPlaceholderType())
        {
            case PLACEHOLDER_INPUT:
            case PLACEHOLDER_ARRAY_NAME:
                ss <<  "array name";
                if (placeholder->getPlaceholderType() == PLACEHOLDER_INPUT)
                    ss << " or array operator";
                break;

            case PLACEHOLDER_ATTRIBUTE_NAME:
                ss <<  "attribute name";
                break;

            case PLACEHOLDER_CONSTANT:
                if (placeholder->getRequiredType().typeId() == TID_VOID)
                    ss <<  "constant";
                else
                    ss << "constant with type '" << placeholder->getRequiredType().typeId() << "'";
                break;

            case PLACEHOLDER_DIMENSION_NAME:
                ss <<  "dimension name";
                break;

            case PLACEHOLDER_EXPRESSION:
                ss <<  "expression";
                break;

            case PLACEHOLDER_SCHEMA:
                ss << "schema";
                break;

            case PLACEHOLDER_AGGREGATE_CALL:
                ss << "aggregate_call";
                break;

            case PLACEHOLDER_END_OF_VARIES:
                ss <<  "end of arguments";
                break;

            default:    SCIDB_UNREACHABLE();
        }
    }

    return ss.str();
}

string Translator::astParamToString(const Node* ast) const
{
    switch (ast->getType())
    {
        default: SCIDB_UNREACHABLE();
        case application:       return "operator (or function)";
        case reference:         return ast->has(referenceArgVersion)     ? "array name" : "reference (array, attribute or dimension name)";
        case schema:            return "schema";
        case cnull:             return "constant with unknown type";
        case creal:             return string("constant with type '") + TID_DOUBLE + "'";
        case cstring:           return string("constant with type '") + TID_STRING + "'";
        case cboolean:          return string("constant with type '") + TID_BOOL   + "'";
        case cinteger:          return string("constant with type '") + TID_INT64  + "'";
    }

    return string();
}

bool Translator::resolveParamAttributeReference(const vector<ArrayDesc> &inputSchemas, shared_ptr<OperatorParamReference> &attRef, bool throwException)
{
    bool found = false;

    size_t inputNo = 0;
    BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
    {
        size_t attributeNo = 0;
        BOOST_FOREACH(const AttributeDesc& attribute, schema.getAttributes())
        {
            if (attribute.getName() == attRef->getObjectName()
             && attribute.hasAlias(attRef->getArrayName()))
            {
                if (found)
                {
                    const string fullName = str(format("%s%s") % (attRef->getArrayName() != "" ? attRef->getArrayName() + "." : "") % attRef->getObjectName() );
                    fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE,attRef) << fullName);
                }
                found = true;

                attRef->setInputNo(inputNo);
                attRef->setObjectNo(attributeNo);
            }
            ++attributeNo;
        }
        ++inputNo;
    }

    if (!found && throwException)
    {
        const string fullName = str(format("%s%s") % (attRef->getArrayName() != "" ? attRef->getArrayName() + "." : "") % attRef->getObjectName() );
        fail(SYNTAX(SCIDB_LE_ATTRIBUTE_NOT_EXIST, attRef) << fullName);
    }

    return found;
}

bool Translator::resolveDimension(const vector<ArrayDesc> &inputSchemas, const string& name, const string& alias,
    size_t &inputNo, size_t &dimensionNo, const ContextPtr &parsingContext, bool throwException)
{
    bool found = false;

    size_t _inputNo = 0;
    BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
    {
        ssize_t _dimensionNo = schema.findDimension(name, alias);
        if (_dimensionNo >= 0)
        {
            if (found)
            {
                const string fullName = str(format("%s%s") % (alias != "" ? alias + "." : "") % name );
                fail(SYNTAX(SCIDB_LE_AMBIGUOUS_DIMENSION, parsingContext) << fullName);
            }
            found = true;

            inputNo = _inputNo;
            dimensionNo = _dimensionNo;
        }
        
        ++_inputNo;
    }

    if (!found && throwException)
    {
        const string fullName = str(format("%s%s") % (alias != "" ? alias + "." : "") % name );
        fail(SYNTAX(SCIDB_LE_DIMENSION_NOT_EXIST, parsingContext) << fullName << "input" << "?");
    }

    return found;
}

bool Translator::resolveParamDimensionReference(const vector<ArrayDesc> &inputSchemas, shared_ptr<OperatorParamReference>& dimRef, bool throwException)
{
    size_t inputNo = 0;
    size_t dimensionNo = 0;

    if (resolveDimension(inputSchemas, dimRef->getObjectName(), dimRef->getArrayName(), inputNo, dimensionNo, newParsingContext(dimRef), throwException))
    {
        dimRef->setInputNo(inputNo);
        dimRef->setObjectNo(dimensionNo);
        return true;
    }

    return false;
}

shared_ptr<OperatorParamAggregateCall> Translator::passAggregateCall(const Node* ast, const vector<ArrayDesc> &inputSchemas)
{
    if (ast->get(applicationArgOperands)->getSize() != 1)
    {
        fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,ast));
    }

    const Node* const arg = ast->get(applicationArgOperands)->get(listArg0);

    shared_ptr<OperatorParam> opParam;

    if (arg->is(reference))
    {
        shared_ptr <AttributeReference> argument = static_pointer_cast<AttributeReference>(onAttributeReference(arg));

        opParam = make_shared<OperatorParamAttributeReference>( newParsingContext(arg),
                                                                argument->getArrayName(),
                                                                argument->getAttributeName(),
                                                                true );

        resolveParamAttributeReference(inputSchemas, (shared_ptr<OperatorParamReference>&) opParam, true);
    }
    else
    if (arg->is(asterisk))
    {
        opParam = make_shared<OperatorParamAsterisk>(newParsingContext(arg));
    }
    else
    {
        fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENT, ast));
    }

    return make_shared<OperatorParamAggregateCall> (
            newParsingContext(ast),
            getStringApplicationArgName(ast),
            opParam,
            getString(ast,applicationArgAlias));
}

bool Translator::placeholdersVectorContainType(const vector<shared_ptr<OperatorParamPlaceholder> > &placeholders,
    OperatorParamPlaceholderType placeholderType)
{
    BOOST_FOREACH(const shared_ptr<OperatorParamPlaceholder> &placeholder, placeholders)
    {
        if (placeholder->getPlaceholderType() == placeholderType)
            return true;
    }
    return false;
}

LQPNPtr Translator::passSelectStatement(const Node *ast)
{
    LQPNPtr result;

    const Node* const fromClause = ast->get(selectArrayArgFromClause);
    const Node* const selectList = ast->get(selectArrayArgSelectList);
    const Node* const grwClause  = ast->get(selectArrayArgGRWClause);

    if (fromClause)
    {
        //First of all joins,scan or nested query will be translated and used
        result = passJoins(fromClause);

        //Next WHERE clause
        const Node *filterClause = ast->get(selectArrayArgFilterClause);
        if (filterClause)
        {
            result = passFilterClause(filterClause, result);
        }

        const Node *orderByClause = ast->get(selectArrayArgOrderByClause);
        if (orderByClause)
        {
            result = passOrderByClause(orderByClause, result);
        }

        result = passSelectList(result, selectList, grwClause);
    }
    else
    {
        if (selectList->getSize() > 1
            ||  selectList->get(listArg0)->is(asterisk)
            || !selectList->get(listArg0)->get(namedExprArgExpr)->is(application)
            || !AggregateLibrary::getInstance()->hasAggregate(getStringApplicationArgName(selectList->get(listArg0)->get(namedExprArgExpr))))
        {
             fail(SYNTAX(SCIDB_LE_AGGREGATE_EXPECTED,selectList));
        }

        const Node* aggregate = selectList->get(listArg0)->get(namedExprArgExpr);
        const chars funcName  = getStringApplicationArgName(aggregate);
        const Node* funcParams = aggregate->get(applicationArgOperands);

        if (funcParams->getSize() != 1)
        {
            fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,funcParams));
        }

        LQPNPtr aggInput;

        switch (funcParams->get(listArg0)->getType())
        {
            case reference:         aggInput = passImplicitScan(funcParams->get(listArg0));   break;
            case selectArray:   aggInput = passSelectStatement(funcParams->get(listArg0));break;
            default:                fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENT2,funcParams->get(listArg0)));
        }

        // First of all try to convert it as select agg(*) from A group by x as G
        // Let's check if asterisk supported
        bool asteriskSupported = true;
        try
        {
            AggregateLibrary::getInstance()->createAggregate(funcName, TypeLibrary::getType(TID_VOID));
        }
        catch (const UserException &e)
        {
            if (SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_ASTERISK == e.getLongErrorCode())
            {
                asteriskSupported = false;
            }
            else
            {
                throw;
            }
        }

        const ArrayDesc &aggInputSchema = aggInput->inferTypes(_qry);
        shared_ptr<OperatorParamAggregateCall> aggCallParam;

        if (asteriskSupported)
        {
            Node *aggregateCallAst = _fac.newApp(aggregate->getWhere(),
                        funcName,
                        _fac.newNode(asterisk,funcParams->get(listArg0)->getWhere()));
            aggCallParam = passAggregateCall(aggregateCallAst, vector<ArrayDesc>(1, aggInputSchema));
        }
        else
        {
            if (aggInputSchema.getAttributes(true).size() == 1)
            {
                size_t attNo = aggInputSchema.getEmptyBitmapAttribute() && aggInputSchema.getEmptyBitmapAttribute()->getId() == 0 ? 1 : 0;

                Node *aggregateCallAst = _fac.newApp(aggregate->getWhere(),funcName,
                            _fac.newRef(funcParams->get(listArg0)->getWhere(),
                                _fac.newString(funcParams->get(listArg0)->getWhere(),aggInputSchema.getAttributes()[attNo].getName())));
                aggCallParam = passAggregateCall(
                            aggregateCallAst,
                            vector<ArrayDesc>(1, aggInputSchema));
            }
            else
            {
                fail(SYNTAX(SCIDB_LE_SINGLE_ATTRIBUTE_IN_INPUT_EXPECTED,funcParams->get(listArg0)));
            }
        }
        aggCallParam->setAlias( getString(selectList->get(listArg0),namedExprArgName));
        LogicalOperator::Parameters aggParams;
        aggParams.push_back(aggCallParam);
        result = appendOperator(aggInput, "aggregate", aggParams, newParsingContext(aggregate));
    }

    if (const Node* intoClause = ast->get(selectArrayArgIntoClause))
    {
        result = passIntoClause(intoClause,result);
    }

    return result;
}

LQPNPtr Translator::passJoins(const Node *ast)
{
    // Left part holding result of join constantly but initially it empty
    LQPNPtr left;

    // Loop for joining all inputs consequentially. Left joining part can be joined previously or
    // empty nodes. Right part will be joined to left on every iteration.
    BOOST_FOREACH(Node *joinItem, ast->getList())
    {
        LQPNPtr right = passJoinItem(joinItem);

        // If we on first iteration - right part turning into left, otherwise left and right parts
        // joining and left part turning into join result.
        if (!left)
        {
            left = right;
        }
        else
        {
            LQPNPtr node = make_shared<LogicalQueryPlanNode>(newParsingContext(joinItem),
                    OperatorLibrary::getInstance()->createLogicalOperator("join"));
            node->addChild(left);
            node->addChild(right);
            left = node;
        }
    }

    //Check JOIN with its inferring
    try
    {
        left->inferTypes(_qry);
    }
    catch(const Exception &e)
    {
        fail(CONV_TO_USER_QUERY_EXCEPTION(e, newParsingContext(ast)));
    }

    // Ok, return join result
    return left;
}

LQPNPtr Translator::passGeneralizedJoin(const Node* ast)
{
    LOG4CXX_TRACE(logger, "Translating JOIN-ON clause...");

    LQPNPtr left = passJoinItem(ast->get(joinClauseArgLeft));
    LQPNPtr right = passJoinItem(ast->get(joinClauseArgRight));

    vector<ArrayDesc> inputSchemas;
    inputSchemas.push_back(left->inferTypes(_qry));
    inputSchemas.push_back(right->inferTypes(_qry));

    vector<shared_ptr<OperatorParamReference> > opParams;
    // Checking JOIN-ON clause for pure DD join
    Node* joinOnAst = ast->get(joinClauseArgExpr);
    bool pureDDJoin = passGeneralizedJoinOnClause(opParams, joinOnAst);

    // Well it looks like DD-join but there is a probability that we have attributes or
    // duplicates in expression. Let's check it.
    for (size_t i = 0; pureDDJoin && (i < opParams.size()); i += 2)
    {
        LOG4CXX_TRACE(logger, "Probably pure DD join");

        bool isLeftDimension = resolveParamDimensionReference(inputSchemas, opParams[i], false);
        bool isLeftAttribute = resolveParamAttributeReference(inputSchemas, opParams[i], false);

        bool isRightDimension = resolveParamDimensionReference(inputSchemas, opParams[i + 1], false);
        bool isRightAttribute = resolveParamAttributeReference(inputSchemas, opParams[i + 1], false);

        const string leftFullName = str(format("%s%s") % (opParams[i]->getArrayName() != "" ?
                opParams[i]->getArrayName() + "." : "") % opParams[i]->getObjectName() );

        const string rightFullName = str(format("%s%s") % (opParams[i + 1]->getArrayName() != "" ?
                opParams[i + 1]->getArrayName() + "." : "") % opParams[i + 1]->getObjectName() );

        // Generic checks on existing and ambiguity first of all
        if (!isLeftDimension && !isLeftAttribute)
        {
            fail(SYNTAX(SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,opParams[i]) << leftFullName);
        }
        else if (isLeftDimension && isLeftAttribute)
        {
            fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,opParams[i]) << leftFullName);
        }

        if (!isRightDimension && !isRightAttribute)
        {
            fail(SYNTAX(SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,opParams[i + 1]) << rightFullName);
        }
        else if (isRightDimension && isRightAttribute)
        {
            fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,opParams[i + 1]) << rightFullName);
        }

        // No chance. There are attributes and we can not do 'SELECT * FROM A JOIN B ON A.x = A.x' with CROSS_JOIN
        if (isRightAttribute || isLeftAttribute || (opParams[i]->getInputNo() == opParams[i + 1]->getInputNo()))
        {
            LOG4CXX_TRACE(logger, "Nope. This is generalized JOIN");
            pureDDJoin = false;
            break;
        }

        //Ensure dimensions ordered by input number
        if (opParams[i]->getInputNo() == 1)
        {
            LOG4CXX_TRACE(logger, "Swapping couple of dimensions");

            shared_ptr<OperatorParamReference> newRight = opParams[i];
            opParams[i] = opParams[i+1];
            opParams[i+1] = newRight;

            isLeftAttribute = isRightAttribute;
            isRightAttribute = isLeftAttribute;

            isLeftDimension = isRightDimension;
            isRightDimension = isLeftDimension;
        }
    }

    if (pureDDJoin)
    {
        LOG4CXX_TRACE(logger, "Yep. This is really DD join. Inserting CROSS_JOIN");
        // This is DD join! We can do it fast with CROSS_JOIN
        LQPNPtr crossJoinNode = make_shared<LogicalQueryPlanNode>(newParsingContext(ast),
                OperatorLibrary::getInstance()->createLogicalOperator("cross_join"));

        crossJoinNode->addChild(left);
        crossJoinNode->addChild(right);
        crossJoinNode->getLogicalOperator()->setParameters(vector<shared_ptr<OperatorParam> >(opParams.begin(), opParams.end()));

        return crossJoinNode;
    }
    else
    {
        LOG4CXX_TRACE(logger, "Inserting CROSS");

        // This is generalized join. Emulating it with CROSS_JOIN+FILTER
        LQPNPtr crossNode = make_shared<LogicalQueryPlanNode>(newParsingContext(ast),
                OperatorLibrary::getInstance()->createLogicalOperator("Cross_Join"));

        crossNode->addChild(left);
        crossNode->addChild(right);

        LOG4CXX_TRACE(logger, "Inserting FILTER");
        vector<shared_ptr<OperatorParam> > filterParams(1);
        filterParams[0] = make_shared<OperatorParamLogicalExpression>(
                            newParsingContext(joinOnAst),
                            AstToLogicalExpression(joinOnAst),
                            TypeLibrary::getType(TID_BOOL));

        return appendOperator(crossNode, "filter", filterParams, newParsingContext(joinOnAst));
    }
}

bool Translator::passGeneralizedJoinOnClause(vector<shared_ptr<OperatorParamReference> > &params,const Node *ast)
{
    if (ast->is(application))
    {
        const string funcName  = getStringApplicationArgName(ast);
        const Node* funcParams = ast->get(applicationArgOperands);

        if (funcName == "and")
        {
            return passGeneralizedJoinOnClause(params,funcParams->get(listArg0))
                && passGeneralizedJoinOnClause(params,funcParams->get(listArg1));
        }
        else if (funcName == "=")
        {
            BOOST_FOREACH(const Node *ref, funcParams->getList())
            {
                if (!ref->is(reference))
                {
                    return false;
                }
            }

            const Node *leftDim  = funcParams->get(listArg0);
            const Node *rightDim = funcParams->get(listArg1);

            const string leftObjectName = getStringReferenceArgName(leftDim);
            const string leftArrayName  = getStringReferenceArgArrayName(leftDim);

            params.push_back(make_shared<OperatorParamDimensionReference>(
                    newParsingContext(leftDim),
                    leftArrayName,
                    leftObjectName,
                    true));

            const string rightObjectName = getStringReferenceArgName(rightDim);
            const string rightArrayName  = getStringReferenceArgArrayName(rightDim);

            params.push_back(make_shared<OperatorParamDimensionReference>(
                    newParsingContext(rightDim),
                    rightArrayName,
                    rightObjectName,
                    true));

            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }
}

LQPNPtr Translator::passCrossJoin(const Node *ast)
{
    LQPNPtr left  = passJoinItem(ast->get(joinClauseArgLeft));
    LQPNPtr right = passJoinItem(ast->get(joinClauseArgRight));
    LQPNPtr node  = make_shared<LogicalQueryPlanNode>(newParsingContext(ast),OperatorLibrary::getInstance()->createLogicalOperator("Cross_Join"));
    node->addChild(left);
    node->addChild(right);

    return node;
}

LQPNPtr Translator::passJoinItem(const Node *ast)
{
    switch (ast->getType())
    {
        case namedExpr:
        {
            Node* expr = ast->get(namedExprArgExpr);

            if (   !expr->is(application)
                && !expr->is(reference)
                && !expr->is(selectArray))
            {
                fail(SYNTAX(SCIDB_LE_INPUT_EXPECTED,expr));
            }

            LQPNPtr result(AstToLogicalPlan(expr));
            prohibitDdl(result);
            prohibitNesting(result);

            if (expr->is(reference) && expr->has(referenceArgAlias))
            {
                result->getLogicalOperator()->setAliasName(getString(expr,referenceArgAlias));
            }

            result->getLogicalOperator()->setAliasName(getString(ast,namedExprArgName));
            return result;
        }

        case joinClause:
            if (ast->has(joinClauseArgExpr))
            {
                return passGeneralizedJoin(ast);
            }
            else
            {
                return passCrossJoin(ast);
            }

        case thinClause:    return passThinClause(ast);
        default:            SCIDB_UNREACHABLE();
                            return LQPNPtr();
    }
}

LQPNPtr Translator::passImplicitScan(const Node *ast)
{
    assert(ast->is(reference));
    LogicalOperator::Parameters scanParams;
    shared_ptr<OperatorParamArrayReference> ref = createArrayReferenceParam(ast, true);
    scanParams.push_back(ref);
    shared_ptr<LogicalOperator> scanOp = OperatorLibrary::getInstance()->createLogicalOperator(
        (ref->getVersion() == ALL_VERSIONS) ? "allversions" : "scan" , getString(ast,referenceArgAlias));
    scanOp->setParameters(scanParams);
    return make_shared<LogicalQueryPlanNode>(newParsingContext(ast), scanOp);
}

LQPNPtr Translator::passFilterClause(const Node* ast, const LQPNPtr &input)
{
    LogicalOperator::Parameters filterParams;
    const ArrayDesc &inputSchema = input->inferTypes(_qry);

    LEPtr lExpr = AstToLogicalExpression(ast);

    checkLogicalExpression(vector<ArrayDesc>(1, inputSchema), ArrayDesc(), lExpr);

    filterParams.push_back(make_shared<OperatorParamLogicalExpression>(newParsingContext(ast),
        lExpr, TypeLibrary::getType(TID_BOOL)));

    shared_ptr<LogicalOperator> filterOp = OperatorLibrary::getInstance()->createLogicalOperator("filter");
    filterOp->setParameters(filterParams);

    LQPNPtr result = make_shared<LogicalQueryPlanNode>(newParsingContext(ast), filterOp);
    result->addChild(input);
    return result;
}

LQPNPtr Translator::passOrderByClause(const Node* ast, const LQPNPtr &input)
{
    LogicalOperator::Parameters sortParams;sortParams.reserve(ast->getSize());
    const ArrayDesc &inputSchema = input->inferTypes(_qry);

    BOOST_FOREACH(const Node* sortAttributeAst,ast->getList())
    {
        shared_ptr<OperatorParamAttributeReference> sortParam = make_shared<OperatorParamAttributeReference>(
            newParsingContext(sortAttributeAst),
            getStringReferenceArgArrayName(sortAttributeAst),
            getStringReferenceArgName(sortAttributeAst),
            true);

        sortParam->setSortAscent(getInteger(sortAttributeAst,referenceArgOrder,ascending) == ascending);

        resolveParamAttributeReference(vector<ArrayDesc>(1, inputSchema), (shared_ptr<OperatorParamReference>&) sortParam, true);

        sortParams.push_back(sortParam);
    }

    LQPNPtr result = appendOperator(input, "sort", sortParams, newParsingContext(ast));
    result->inferTypes(_qry);
    return result;
}

LQPNPtr Translator::passIntoClause(const Node* ast, LQPNPtr &input)
{
    LOG4CXX_TRACE(logger, "Translating INTO clause...");

    const ArrayDesc   inputSchema    = input->inferTypes(_qry);
    const string      targetName     = ast->getString();
    const ContextPtr& parsingContext = newParsingContext(ast);

    LQPNPtr result;

    LogicalOperator::Parameters targetParams(1,make_shared<OperatorParamArrayReference>(parsingContext,"",targetName,true));
    shared_ptr<LogicalOperator> storeOp;

    if (!SystemCatalog::getInstance()->containsArray(targetName))
    {
        LOG4CXX_TRACE(logger, str(format("Target array '%s' not existing so inserting STORE") % targetName));
        storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
        storeOp->setParameters(targetParams);
        result = make_shared<LogicalQueryPlanNode>(parsingContext, storeOp);
        result->addChild(input);
    }
    else
    {
        LOG4CXX_TRACE(logger, str(format("Target array '%s' existing.") % targetName));

        ArrayDesc destinationSchema;
        SystemCatalog::getInstance()->getArrayDesc(targetName, destinationSchema);

        /*
         * Let's check if input can fit somehow into destination array. If names differ we can insert
         * CAST. If array partitioning differ we can insert REPART. Also we can force input to be empty
         * array to fit empty destination. We can't change array size or dimensions/attributes types,
         * so if such difference found - we skipping casting/repartioning.
         */
        LQPNPtr fittedInput = fitInput(input, destinationSchema);
        bool tryFlip = false;
        try
        {
            LOG4CXX_TRACE(logger, "Trying to insert STORE");
            storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
            storeOp->setParameters(targetParams);
            result = make_shared<LogicalQueryPlanNode>(parsingContext, storeOp);
            result->addChild(fittedInput);
            result->inferTypes(_qry);
        }
        catch (const UserException& e)
        {
            if (SCIDB_SE_INFER_SCHEMA == e.getShortErrorCode())
            {
                LOG4CXX_TRACE(logger, "Can not infer schema from REPART and/or CAST and/or STORE");
                tryFlip = true;
            }
            else
            {
                LOG4CXX_TRACE(logger, "Something going wrong");
                throw;
            }
        }

        if (!tryFlip)
        {
            LOG4CXX_TRACE(logger, "OK. We managed to fit input into destination. STORE will be used.");
            return result;
        }

        try
        {
            LOG4CXX_TRACE(logger, "Trying to wrap with STORE(REDIMENSION(...))");

            {
            shared_ptr<LogicalOperator> redimOp = OperatorLibrary::getInstance()->createLogicalOperator("redimension");
            redimOp->setParameters(LogicalOperator::Parameters(1,make_shared<OperatorParamSchema>(parsingContext, destinationSchema)));
            result = make_shared<LogicalQueryPlanNode>(parsingContext, redimOp);
            result->addChild(input);
            result->inferTypes(_qry);
            }

            {
            shared_ptr<LogicalOperator> storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
            storeOp->setParameters(targetParams);
            LQPNPtr storeNode = make_shared<LogicalQueryPlanNode>(parsingContext, storeOp);
            storeNode->addChild(result);
            return storeNode;
            }
        }
        catch (const UserException& e)
        {
            if (SCIDB_SE_INFER_SCHEMA == e.getShortErrorCode())
            {
                LOG4CXX_TRACE(logger, "Can not infer schema from REDIMENSION_STORE");
                fail(SYNTAX(SCIDB_LE_CAN_NOT_STORE,ast) << targetName);
            }

            LOG4CXX_TRACE(logger, "Something going wrong");
            throw;
        }
        LOG4CXX_TRACE(logger, "OK. REDIMENSION_STORE matched.");
    }

    return result;
}

LQPNPtr Translator::passUpdateStatement(const Node *ast)
{
    Node *arrayRef = ast->get(updateArrayArgArrayRef);
    LQPNPtr result = passImplicitScan(arrayRef);

    const string arrayName = getStringReferenceArgName(arrayRef);

    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);
    const Node *updateList = ast->get(updateArrayArgUpdateList);

    map<string,string> substMap;

    LogicalOperator::Parameters applyParams;
    unsigned int counter = 0;
    BOOST_FOREACH(const Node *updateItem, updateList->getList())
    {
        const string attName = getString(updateItem,updateArgName);

        bool found = false;
        BOOST_FOREACH(const AttributeDesc &att, arrayDesc.getAttributes())
        {
            if (att.getName() == attName)
            {
                const string newAttName = genUniqueObjectName("updated_" + attName, counter, vector<ArrayDesc>(1, arrayDesc), true);
                substMap[att.getName()] = newAttName;
                found = true;

                //placeholder
                Node* attExpr = updateItem->get(updateArgExpr);

                vector<ArrayDesc> schemas;
                schemas.push_back(arrayDesc);

                if (expressionType(AstToLogicalExpression(attExpr), _qry, schemas) != TypeLibrary::getType(att.getType()).typeId())
                {
                    //Wrap expression with type converter appropriate attribute type
                    attExpr = _fac.newApp(
                                attExpr->getWhere(),
                               _fac.newString(attExpr->getWhere(),TypeLibrary::getType(att.getType()).name()),
                               _fac.newCopy(attExpr));
                }

                /* Converting WHERE predicate into iif application in aply operator parameter:
                 * apply(A, a_updated, iif(iif(is_null(whereExpr), false, whereExpr), updateExpr, a))
                 * If we have where clause, we must check if value of where predicate is null or not.
                 * Value of attribute changed only when result of predicate is TRUE. If result of
                 * predicate is NULL or FALSE, value of attribute will not be changed.
                 */

                if (const Node* const whereExpr = ast->get(updateArrayArgWhereClause))
                {
                    location w = attExpr->getWhere();
                    attExpr    = _fac.newApp(w,"iif",
                                 _fac.newApp(w,"iif",
                                 _fac.newApp(w,"is_null",_fac.newCopy(whereExpr)),
                                 _fac.newBoolean(w,false),
                                 _fac.newCopy(whereExpr)),
                                 _fac.newCopy(attExpr),
                                 _fac.newRef(w,_fac.newString(w,attName)));
                }

                applyParams.push_back(make_shared<OperatorParamAttributeReference>(newParsingContext(updateItem),
                        "", newAttName, false));

                applyParams.push_back(make_shared<OperatorParamLogicalExpression>(newParsingContext(updateItem),
                        AstToLogicalExpression(attExpr), TypeLibrary::getType(att.getType()), false));

                break;
            }
        }

        if (!found)
        {
            fail(SYNTAX(SCIDB_LE_ATTRIBUTE_NOT_EXIST,updateItem->get(updateArgName)) << attName);
        }
    }

    //Wrap child nodes with apply operator created new attribute
    result = appendOperator(result, "apply", applyParams, newParsingContext(updateList));

    //Projecting changed attributes along with unchanged to simulate real update
    vector<ArrayDesc> schemas;
    schemas.push_back(result->inferTypes(_qry));
    LogicalOperator::Parameters projectParams;
    BOOST_FOREACH(const AttributeDesc &att, arrayDesc.getAttributes())
    {
        shared_ptr<OperatorParamReference> newAtt;
        if (substMap[att.getName()] != "")
        {
            newAtt = make_shared<OperatorParamAttributeReference>(newParsingContext(updateList),
                    "", substMap[att.getName()], true);
        }
        else
        {
            newAtt = make_shared<OperatorParamAttributeReference>(newParsingContext(updateList),
                    "", att.getName(), true);
        }
        resolveParamAttributeReference(schemas, newAtt);
        projectParams.push_back(newAtt);
    }

    shared_ptr<LogicalOperator> projectOp = OperatorLibrary::getInstance()->createLogicalOperator("project");
    projectOp->setParameters(projectParams);

    LQPNPtr projectNode = make_shared<LogicalQueryPlanNode>(newParsingContext(updateList), projectOp);
    projectNode->addChild(result);
    result = projectNode;

    //Finally wrap input with STORE operator
    LogicalOperator::Parameters storeParams;

    storeParams.push_back(make_shared<OperatorParamArrayReference>(
            newParsingContext(getReferenceArgName(arrayRef)), "", arrayName, true));

    shared_ptr<LogicalOperator> storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
    storeOp->setParameters(storeParams);

    LQPNPtr storeNode = make_shared<LogicalQueryPlanNode>(newParsingContext(ast), storeOp);
    storeNode->addChild(result);
    result = storeNode;

    return result;
}

LQPNPtr Translator::passInsertIntoStatement(const Node *ast)
{
    assert(ast->is(insertArray));
    LOG4CXX_TRACE(logger, "Translating INSERT INTO");

    const Node* srcAst = ast->get(insertArrayArgSource);
    const Node* dstAst = ast->get(insertArrayArgDestination);

    string dstName = dstAst->getString();
    LogicalOperator::Parameters dstOpParams;
    dstOpParams.push_back(make_shared<OperatorParamArrayReference>(newParsingContext(dstAst), "", dstName, true));
    if (!SystemCatalog::getInstance()->containsArray(dstName))
    {
        fail(QPROC(SCIDB_LE_ARRAY_DOESNT_EXIST, dstAst) << dstName);
    }

    ArrayDesc dstSchema;
    SystemCatalog::getInstance()->getArrayDesc(dstName, dstSchema);

    LQPNPtr srcNode;
    if (srcAst->is(selectArray))
    {
        LOG4CXX_TRACE(logger, "Source of INSERT INTO is SELECT");
        srcNode = passSelectStatement(srcAst);
    }
    else if (srcAst->is(cstring))
    {
        LOG4CXX_TRACE(logger, "Source of INSERT INTO is array literal");
        LogicalOperator::Parameters buildParams;
        buildParams.push_back(make_shared<OperatorParamSchema>(
            newParsingContext(dstAst),
            dstSchema));

        const string arrayLiteral = srcAst->getString();
        Value sval(TypeLibrary::getType(TID_STRING));
        sval.setData(arrayLiteral.c_str(), arrayLiteral.length() + 1);
        LEPtr expr = make_shared<Constant>(newParsingContext(ast), sval, TID_STRING);
        buildParams.push_back(make_shared<OperatorParamLogicalExpression>(
            newParsingContext(srcAst),
            expr, TypeLibrary::getType(TID_STRING), true));

        Value bval(TypeLibrary::getType(TID_BOOL));
        bval.setBool(true);
        expr = make_shared<Constant>(newParsingContext(ast), bval, TID_BOOL);
        buildParams.push_back(make_shared<OperatorParamLogicalExpression>(
            newParsingContext(srcAst),
            expr, TypeLibrary::getType(TID_BOOL), true));

        srcNode = make_shared<LogicalQueryPlanNode>(
                newParsingContext(srcAst),
                OperatorLibrary::getInstance()->createLogicalOperator("build"));
        srcNode->getLogicalOperator()->setParameters(buildParams);
    }
    else
    {
        SCIDB_UNREACHABLE();
    }

    LOG4CXX_TRACE(logger, "Checking source schema and trying to fit it to destination for inserting");
    srcNode = fitInput(srcNode, dstSchema);

    LOG4CXX_TRACE(logger, "Inserting INSERT operator");
    return appendOperator(srcNode, "insert", dstOpParams, newParsingContext(ast));
}

void Translator::checkLogicalExpression(const vector<ArrayDesc> &inputSchemas, const ArrayDesc &outputSchema,const LEPtr &expr)
{
    if (typeid(*expr) == typeid(AttributeReference))
    {
        const shared_ptr<AttributeReference> &ref = static_pointer_cast<AttributeReference>(expr);

        //We don't know exactly what type this reference, so check both attribute and dimension,
        //and if we eventually found both, so throw ambiguous exception

        const bool foundAttrIn  = checkAttribute(inputSchemas,    ref->getArrayName(), ref->getAttributeName(), ref->getParsingContext());
        const bool foundAttrOut = checkAttribute(vector<ArrayDesc>(1, outputSchema),   ref->getArrayName(),     ref->getAttributeName(), ref->getParsingContext());
        const bool foundDimIn   = checkDimension(inputSchemas,    ref->getArrayName(), ref->getAttributeName(), ref->getParsingContext());
        const bool foundDimOut  = checkDimension(vector<ArrayDesc>(1, outputSchema),    ref->getArrayName(),     ref->getAttributeName(), ref->getParsingContext());

        // Checking ambiguous situation in input schema. If no ambiguity we found dimension/attribute
        // or not.
        if (foundAttrIn && foundDimIn)
        {
            const string fullName = str(format("%s%s") % (ref->getArrayName() != "" ? ref->getArrayName() + "." : "") % ref->getAttributeName() );
            fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,ref) << fullName);
        }
        // If we can't find references in input schema, checking output schema.
        else if (!(foundAttrIn || foundDimIn))
        {
            // Same as for input: checking ambiguity in output schema.
            if (foundAttrOut && foundDimOut)
            {
                const string fullName = str(format("%s%s") % (ref->getArrayName() != "" ? ref->getArrayName() + "." : "") % ref->getAttributeName() );
                fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,ref) << fullName);
            }
            // If we can't find reference even in output schema, finally throw error
            else if (!(foundAttrOut || foundDimOut))
            {
                ArrayDesc schema;
                if (ref->getArrayName() != "" || !SystemCatalog::getInstance()->getArrayDesc(ref->getAttributeName(), schema, false) || schema.getAttributes(true).size() != 1 || schema.getDimensions().size() != 1 || schema.getDimensions()[0].getLength() != 1)
                {
                    const string fullName = str(format("%s%s") % (ref->getArrayName() != "" ? ref->getArrayName() + "." : "") % ref->getAttributeName() );
                    fail(SYNTAX(SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,ref) << fullName);
                }
            }
        }
                // If no ambiguity, and found some reference, we ignoring all from output schema.
    }
    else if (typeid(*expr) == typeid(Function))
    {
        const shared_ptr<Function> &func = static_pointer_cast<Function>(expr);
        BOOST_FOREACH(const LEPtr &funcArg, func->getArgs())
        {
            checkLogicalExpression(inputSchemas, outputSchema, funcArg);
        }
    }
}

bool Translator::checkAttribute(const vector<ArrayDesc> &inputSchemas,const string& aliasName,const string& attributeName,const ContextPtr &ctxt)
{
    bool found = false;
    size_t schemaNo = 0;
    size_t attNo = 0;

    BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
    {
        BOOST_FOREACH(const AttributeDesc& attribute, schema.getAttributes())
        {
            if (attribute.getName() == attributeName && attribute.hasAlias(aliasName))
            {
                if (found)
                {
                    const string fullName = str(format("%s%s") % (aliasName != "" ? aliasName + "." : "") % attributeName);
                    fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE, ctxt) << fullName);
                }
                found = true;
            }
            ++attNo;
        }
        attNo = 0;
        ++schemaNo;
    }

    return found;
}

bool Translator::checkDimension(const vector<ArrayDesc> &inputSchemas, const string& aliasName, const string& dimensionName,const ContextPtr &ctxt)
{
    bool found = false;
    BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
    {
        BOOST_FOREACH(const DimensionDesc& dim, schema.getDimensions())
        {
            if (dim.hasNameAndAlias(dimensionName, aliasName))
            {
                if (found)
                {
                    const string fullName = str(format("%s%s") % (aliasName != "" ? aliasName + "." : "") % dimensionName);
                    fail(SYNTAX(SCIDB_LE_AMBIGUOUS_DIMENSION, ctxt) << fullName);
                }
                found = true;
            }
        }
    }

    return found;
}

LQPNPtr Translator::appendOperator(const LQPNPtr &node,const string& opName,const LogicalOperator::Parameters &opParams,const ContextPtr &opParsingContext)
{
    LQPNPtr newNode = make_shared<LogicalQueryPlanNode>(
            opParsingContext,
            OperatorLibrary::getInstance()->createLogicalOperator(opName));
    newNode->getLogicalOperator()->setParameters(opParams);
    newNode->addChild(node);
    return newNode;
}

bool Translator::astHasUngroupedReferences(const Node* ast,const set<string>& grouped) const
{
    switch (ast->getType())
    {
        case application:
        {
            BOOST_FOREACH(const Node* a,ast->getList(applicationArgOperands))
            {
                if (astHasUngroupedReferences(a,grouped))
                    return true;
            }

            return false;
        }

        case reference: return grouped.find(getStringReferenceArgName(ast)) == grouped.end();
        case asterisk:  return true;
        default:        return false;
    }
}

bool Translator::astHasAggregates(const Node* ast) const
{
    switch (ast->getType())
    {
        case olapAggregate: ast = ast->get(olapAggregateArgApplication); // and fall through
        case application:
        {
            if (AggregateLibrary::getInstance()->hasAggregate(getStringApplicationArgName(ast)))
                return true;

            BOOST_FOREACH(const Node* a,ast->getList(applicationArgOperands))
            {
                if (astHasAggregates(a))
                    return true;
            }

            return false;
        }

        default:  return false;
    }
}

Node* Translator::decomposeExpression(
    const Node*        ast,
    vector<Node*>&     preAggregationEvals,
    vector<Node*>&     aggregateFunctions,
    unsigned int&      internalNameCounter,
    bool               hasAggregates,
    const ArrayDesc&   inputSchema,
    const set<string>& groupedDimensions,
    bool               window,
    bool&              joinOrigin)
{
    LOG4CXX_TRACE(logger, "Decomposing expression");
    vector<ArrayDesc> inputSchemas(1, inputSchema);

    switch (ast->getType())
    {
        case application:
        case olapAggregate:
        {
            LOG4CXX_TRACE(logger, "This is function");

            const Node* funcNode = ast->is(application)
                ? ast
                : ast->get(olapAggregateArgApplication);

            const chars funcName = getStringApplicationArgName(funcNode);
            const Node* funcArgs = funcNode->get(applicationArgOperands);

            bool isAggregate = AggregateLibrary::getInstance()->hasAggregate(funcName);
            bool isScalar    = FunctionLibrary::getInstance()->hasFunction(funcName, false);

            if (isAggregate && isScalar)
            {
                shared_ptr<Expression> pExpr = make_shared<Expression>();
                try
                {
                    ArrayDesc outputSchema;
                    pExpr->compile(AstToLogicalExpression(ast), _qry, false, TID_VOID, inputSchemas, outputSchema);
                    isAggregate = false;
                }
                catch (const Exception &e)
                {}
            }

            // We found aggregate and must care of it
            if (isAggregate)
            {
                LOG4CXX_TRACE(logger, "This is aggregate call");
                // Currently framework supports only one argument aggregates so drop any other cases
                if (funcArgs->getSize() != 1)
                {
                    LOG4CXX_TRACE(logger, "Passed too many arguments to aggregate call");
                    fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,funcNode));
                }

                const Node* aggArg = funcArgs->get(listArg0);

                // Check if this sole expression has aggregate calls itself and drop if yes
                if (astHasAggregates(aggArg))
                {
                    LOG4CXX_TRACE(logger, "Nested aggregate");
                    fail(SYNTAX(SCIDB_LE_AGGREGATE_CANT_BE_NESTED,funcNode));
                }

                bool isDimension = false;
                if (aggArg->is(reference))
                {
                    chars dimName;
                    chars dimAlias;
                    passReference(aggArg, dimAlias, dimName);

                    //If we found dimension inside aggregate we must convert into attribute value before aggregating
                    BOOST_FOREACH(const DimensionDesc& dim, inputSchema.getDimensions())
                    {
                        if (dim.hasNameAndAlias(dimName, dimAlias))
                        {
                            isDimension = true;
                            break;
                        }
                    }
                }

                // If function argument is reference or asterisk, we can translate to aggregate call
                // it as is but must assign alias to reference in post-eval expression
                if ((aggArg->is(reference) && !isDimension) ||  aggArg->is(asterisk))
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is reference or asterisk");
                    Node *alias = _fac.newString(
                        funcNode->getWhere(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));
                    if (ast->is(application))
                    {
                        Node* aggFunc = _fac.newCopy(funcNode);
                        aggFunc->set(applicationArgAlias, alias);
                        aggregateFunctions.push_back(aggFunc);
                    }
                    else
                    if (ast->is(olapAggregate))
                    {
                        Node* aggFunc = _fac.newCopy(ast);
                        aggFunc->get(olapAggregateArgApplication)->set(applicationArgAlias, alias);
                        aggregateFunctions.push_back(aggFunc);
                    }
                    else
                    {
                        SCIDB_UNREACHABLE();
                    }

                    // Finally returning reference to aggregate result in overall expression
                    return _fac.newRef(funcNode->get(applicationArgOperands)->getWhere(),_fac.newCopy(alias));
                }
                // Handle select statement
                else
                if (aggArg->is(selectArray))
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is SELECT");
                    fail(SYNTAX(SCIDB_LE_UNEXPECTED_SELECT_INSIDE_AGGREGATE,ast));
                }
                // We found expression or constant. We can't use it in aggregate/regrid/window and
                // must create pre-evaluate
                else
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is expression");
                    // Let's we have aggregate(expression)

                    // Prepare result attribute name for expression which must be evaluated
                    Node *preEvalAttName = _fac.newString(ast->getWhere(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));
                    // Named expression 'expression as resname1' will be later translated into
                    // operator APPLY(input, expression, preEvalAttName)
                    Node *applyExpression = _fac.newNode(namedExpr, ast->getWhere(),_fac.newCopy(ast->get(applicationArgOperands)->get(listArg0)), preEvalAttName);
                    // This is must be evaluated before aggregates
                    preAggregationEvals.push_back(applyExpression);

                    // Prepare result attribute name for aggregate call which must be evaluate before
                    // evaluating whole expression
                    Node *postEvalAttName = _fac.newString(ast->getWhere(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));

                    // Aggregate call will be translated later into AGGREGATE(input, aggregate(preEvalAttName) as postEvalName)
                    Node *aggregateExpression =
                        _fac.newApp(ast->getWhere(),
                        _fac.newCopy(getApplicationArgName(ast)),
                        _fac.newRef(ast->get(applicationArgOperands)->getWhere(),_fac.newCopy(preEvalAttName)));
                    aggregateExpression->set(applicationArgAlias,postEvalAttName);
                    aggregateFunctions.push_back(aggregateExpression);

                    // Finally returning reference to aggregate result in overall expression
                    return _fac.newRef(funcNode->get(applicationArgOperands)->getWhere(),_fac.newCopy(postEvalAttName));
                }
            }
            // This is scalar function. We must pass each argument and construct new function call
            // AST node for post-eval expression
            else
            {
                if (ast->is(olapAggregate))
                {
                    fail(SYNTAX(SCIDB_LE_WRONG_OVER_USAGE,ast));
                }

                LOG4CXX_TRACE(logger, "This is scalar function");
                vector<Node*> newArgs;
                BOOST_FOREACH(const Node* funcArg,funcArgs->getList())
                {
                    LOG4CXX_TRACE(logger, "Passing function argument");
                      newArgs.push_back(
                         decomposeExpression(
                            funcArg,
                            preAggregationEvals,
                            aggregateFunctions,
                            internalNameCounter,
                            hasAggregates,
                            inputSchema,
                            groupedDimensions,
                            window,
                            joinOrigin));
                }

                return _fac.newApp(ast->getWhere(),
                       _fac.newCopy(getApplicationArgName(funcNode)),
                       newArgs);
            }

            break;
        }

        default:
        {
            LOG4CXX_TRACE(logger, "This is reference or constant");
            if (ast->is(reference))
            {
                if (astHasUngroupedReferences(ast, groupedDimensions) && hasAggregates && !window)
                {
                    LOG4CXX_TRACE(logger, "We can not use references in expression with aggregate");
                    fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE,ast));
                }

                bool isDimension = false;
                chars dimName;
                chars dimAlias;
                passReference(ast, dimAlias, dimName);

                BOOST_FOREACH(const DimensionDesc& dim, inputSchema.getDimensions())
                {
                    if (dim.hasNameAndAlias(dimName, dimAlias))
                    {
                        isDimension = true;
                        break;
                    }
                }
                if (window && !isDimension)
                    joinOrigin = true;
            }

            LOG4CXX_TRACE(logger, "Cloning node to post-evaluation expression");
            return _fac.newCopy(ast);

            break;
        }
    }

    SCIDB_UNREACHABLE();
    return NULL;
}

LQPNPtr Translator::passSelectList(
    LQPNPtr &input,
    const Node *const selectList,
    const Node *const grwAsClause)
{
    LOG4CXX_TRACE(logger, "Translating SELECT list");
    const ArrayDesc& inputSchema = input->inferTypes(_qry);
    const vector<ArrayDesc> inputSchemas(1, inputSchema);
    LogicalOperator::Parameters projectParams;
    bool joinOrigin = false;
    const bool isWindowClauses = grwAsClause && grwAsClause->is(list);

    bool selectListHasAggregates = false;
    BOOST_FOREACH(const Node *selItem, selectList->getList())
    {
        if (selItem->is(namedExpr))
        {
            if (astHasAggregates(selItem->get(namedExprArgExpr)))
            {
                selectListHasAggregates = true;
                break;
            }
        }
    }

    if (grwAsClause && !selectListHasAggregates)
    {
        LOG4CXX_TRACE(logger,"GROUP BY, WINDOW, REGRID or REDIMENSION present, but SELECT list does"
                " not contain aggregates");
        fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE,selectList->get(listArg0)));
    }

    //List of objects in GROUP BY or REDIMENSION list. We don't care about ambiguity now it will be done later.
    //In case REGRID or WINDOW we just enumerate all dimensions from input schema
    set<string> groupedDimensions;
    if (grwAsClause)
    {
        switch (grwAsClause->getType())
        {
            case groupByClause:
                BOOST_FOREACH (const Node *dimensionAST, grwAsClause->getList(groupByClauseArgList))
                {
                    groupedDimensions.insert(getStringReferenceArgName(dimensionAST));
                }
                break;
            case redimensionClause:
                BOOST_FOREACH (const Node *dimensionAST, grwAsClause->getList(listArg0))
                {
                    assert(dimensionAST->is(dimension));
                    groupedDimensions.insert(dimensionAST->get(dimensionArgName)->getString());
                }
                break;
            case list:
                assert(isWindowClauses);
            case regridClause:
                BOOST_FOREACH(const DimensionDesc& dim, inputSchema.getDimensions())
                {
                    groupedDimensions.insert(dim.getBaseName());
                    BOOST_FOREACH(const DimensionDesc::NamesPairType &name, dim.getNamesAndAliases())
                    {
                        groupedDimensions.insert(name.first);
                    }
                }
                break;
            default:
                SCIDB_UNREACHABLE();
                break;
        }
    }

    vector<Node*> preAggregationEvals;
    vector<Node*> aggregateFunctions;
    vector<Node*> postAggregationEvals;

    LQPNPtr result = input;

    unsigned int internalNameCounter = 0;
    unsigned int externalExprCounter = 0;
    unsigned int externalAggregateCounter = 0;
    BOOST_FOREACH(Node *selItem, selectList->getList())
    {
        LOG4CXX_TRACE(logger, "Translating SELECT list item");

        switch (selItem->getType())
        {
            case namedExpr:
            {
                LOG4CXX_TRACE(logger, "Item is named expression");

                // If reference is attribute, we must do PROJECT
                bool doProject = false;
                if ( selItem->get(namedExprArgExpr)->is(reference)
                 && !selItem->get(namedExprArgName)
                 && !(grwAsClause && grwAsClause->is(redimensionClause)))
                {
                    const Node* refNode = selItem->get(namedExprArgExpr);
                    const chars name   = getStringReferenceArgName(refNode);
                    const chars alias  = getStringReferenceArgArrayName(refNode);
                    // Strange issue with BOOST_FOREACH infinity loop. Leaving for-loop instead.
                    for(vector<AttributeDesc>::const_iterator attIt = inputSchema.getAttributes().begin();
                        attIt != inputSchema.getAttributes().end(); ++attIt)
                    {
                        LOG4CXX_TRACE(logger, "Item is named expression");
                        if (attIt->getName() == name && attIt->hasAlias(alias))
                        {
                            doProject = true;
                            break;
                        }
                    }
                }

                if (doProject)
                {
                    LOG4CXX_TRACE(logger, "Item is has no name so this is projection");
                    const Node *refNode = selItem->get(namedExprArgExpr);
                    if (selectListHasAggregates && !isWindowClauses)
                    {
                        LOG4CXX_TRACE(logger, "SELECT list contains aggregates so we can't do projection");
                        fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,refNode));
                    }
                    else if (isWindowClauses)
                    {
                        joinOrigin = true;
                    }

                    shared_ptr<OperatorParamReference> param = make_shared<OperatorParamAttributeReference>(
                        newParsingContext(selItem),
                        getStringReferenceArgArrayName(refNode),
                        getStringReferenceArgName(refNode),
                        true);

                    resolveParamAttributeReference(inputSchemas, param);
                    projectParams.push_back(param);
                }
                else
                {
                    LOG4CXX_TRACE(logger, "This is will be expression evaluation");

                    if (astHasAggregates(selItem->get(namedExprArgExpr)))
                    {
                        LOG4CXX_TRACE(logger, "This is will be expression with aggregate evaluation");
                    }
                    else
                    {
                        LOG4CXX_TRACE(logger, "This is will be expression evaluation");
                        if (astHasUngroupedReferences(selItem->get(namedExprArgExpr), groupedDimensions)
                            && selectListHasAggregates && !isWindowClauses)
                        {
                            LOG4CXX_TRACE(logger, "This expression has references we can't evaluate it because we has aggregates");
                            fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,selItem));
                        }
                        else if (isWindowClauses)
                        {
                            joinOrigin = true;
                        }
                    }

                    Node *postEvalExpr = decomposeExpression(
                        selItem->get(namedExprArgExpr),
                        preAggregationEvals,
                        aggregateFunctions,
                        internalNameCounter,
                        selectListHasAggregates,
                        inputSchema,
                        groupedDimensions,
                        isWindowClauses,
                        joinOrigin);

                    // Prepare name for SELECT item result. If AS was used by user, we just copy it
                    // else we generate new name
                    Node* outputNameNode = NULL;
                    if (selItem->get(namedExprArgName))
                    {
                        outputNameNode = _fac.newCopy(selItem->get(namedExprArgName));
                    }
                    else
                    {
                        // If SELECT item is single aggregate we will use function name as prefix
                        // else we will use 'expr' prefix
                        string prefix;
                        if (selItem->get(namedExprArgExpr)->is(application)
                            && AggregateLibrary::getInstance()->hasAggregate(
                                getStringApplicationArgName(selItem->get(namedExprArgExpr))))
                        {
                            outputNameNode = _fac.newString(selItem->getWhere(),
                                genUniqueObjectName(
                                    getStringApplicationArgName(selItem->get(namedExprArgExpr)),
                                    externalAggregateCounter, inputSchemas, false, selectList->getList()));
                        }
                        else if (olapAggregate == selItem->get(namedExprArgExpr)->getType()
                            && AggregateLibrary::getInstance()->hasAggregate(
                                getStringApplicationArgName(selItem->get(namedExprArgExpr)->get(olapAggregateArgApplication))))
                        {
                            Node* funcNode = selItem->get(namedExprArgExpr)->get(olapAggregateArgApplication);
                            outputNameNode = _fac.newString(funcNode->getWhere(),
                                genUniqueObjectName(
                                    getStringApplicationArgName(funcNode),
                                    externalAggregateCounter, inputSchemas, false, selectList->getList()));
                        }
                        else
                        {
                            outputNameNode = _fac.newString(selItem->getWhere(),
                                genUniqueObjectName("expr", externalExprCounter, inputSchemas, false, selectList->getList()));
                        }
                    }

                    Node *postEvalNamedExpr = _fac.newNode(namedExpr, selItem->get(namedExprArgExpr)->getWhere(),postEvalExpr,outputNameNode);

                    postAggregationEvals.push_back(postEvalNamedExpr);

                    projectParams.push_back(make_shared<OperatorParamAttributeReference>(
                        newParsingContext(postEvalNamedExpr->get(namedExprArgName)),
                        "", postEvalNamedExpr->get(namedExprArgName)->getString(), true));
                }

                break;
            }

            case asterisk:
            {
                LOG4CXX_TRACE(logger, "Item is asterisk. It will be expanded to attributes.");

                if (selectListHasAggregates)
                {
                    LOG4CXX_TRACE(logger, "SELECT list contains aggregates so we can't expand asterisk");
                    fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,selItem));
                }

                // We can freely omit project, if asterisk is sole item in selection list
                if (selectList->getSize() == 1)
                {
                    break;
                }

                // Otherwise prepare parameters for project
                BOOST_FOREACH(const AttributeDesc &att, inputSchema.getAttributes(true))
                {
                    shared_ptr<OperatorParamReference> param = make_shared<OperatorParamAttributeReference>(
                        newParsingContext(selItem),
                        "",
                        att.getName(),
                        true);

                    resolveParamAttributeReference(inputSchemas, param);
                    projectParams.push_back(param);
                }
                break;
            }

            default:
            {
                LOG4CXX_TRACE(logger, "Unknown item. Asserting.");
                SCIDB_UNREACHABLE();
            }
        }
    }

    if (preAggregationEvals.size())
    {
        LogicalOperator::Parameters applyParams;
        // Pass list of pre-aggregate evaluations and translate it into APPLY operators
        LOG4CXX_TRACE(logger, "Translating preAggregateEval into logical operator APPLY");
        BOOST_FOREACH(const Node *namedExprNode, preAggregationEvals)
        {
            assert(namedExprNode->is(namedExpr));

            // This is internal output reference which will be used for aggregation
            shared_ptr<OperatorParamReference> refParam = make_shared<OperatorParamAttributeReference>(
                newParsingContext(namedExprNode->get(namedExprArgName)),
                "", namedExprNode->get(namedExprArgName)->getString(), false);

            // This is expression which will be used as APPLY expression
            LEPtr lExpr = AstToLogicalExpression(namedExprNode->get(namedExprArgExpr));
            checkLogicalExpression(inputSchemas, ArrayDesc(), lExpr);
            shared_ptr<OperatorParam> exprParam = make_shared<OperatorParamLogicalExpression>(
                newParsingContext(namedExprNode->get(namedExprArgExpr)),
                lExpr, TypeLibrary::getType(TID_VOID));

            applyParams.push_back(refParam);
            applyParams.push_back(exprParam);
        }
        LOG4CXX_TRACE(logger, "APPLY node appended");
        result = appendOperator(result, "apply", applyParams, newParsingContext(selectList));
    }

    const vector<ArrayDesc> preEvalInputSchemas(1, result->inferTypes(_qry));

    // Pass list of aggregates and create single AGGREGATE/REGRID/WINDOW operator
    if (aggregateFunctions.size() > 0)
    {
        LOG4CXX_TRACE(logger, "Translating aggregate into logical aggregate call");
        //WINDOW can be used multiple times so we have array of parameters for each WINDOW
        map<string, pair<string, LogicalOperator::Parameters> > aggregateParams;

        if (grwAsClause)
        {
            switch (grwAsClause->getType())
            {
                case list:
                {
                    assert(isWindowClauses);
                    LOG4CXX_TRACE(logger, "Translating windows list");
                    BOOST_FOREACH(const Node* windowClause, grwAsClause->getList())
                    {
                        LOG4CXX_TRACE(logger, "Translating window");
                        const Node* ranges = windowClause->get(windowClauseArgRangesList);
                        typedef pair<Node*, Node*> pairOfNodes; //BOOST_FOREACH macro don't like commas
                        vector<pairOfNodes> windowSizes(inputSchema.getDimensions().size(),
                                make_pair<Node*, Node*>(NULL, NULL));

                        size_t inputNo;
                        size_t dimNo;

                        LogicalOperator::Parameters windowParams;
                        LOG4CXX_TRACE(logger, "Translating dimensions of window");
                        bool variableWindow = false;
                        BOOST_FOREACH(const Node* dimensionRange, ranges->getList())
                        {
                            variableWindow = windowClause->get(windowClauseArgVariableWindowFlag)->getBoolean();
                            Node* dimNameClause  = dimensionRange->get(windowDimensionRangeArgName);
                            const chars dimName  = getStringReferenceArgName(dimNameClause);
                            const chars dimAlias = getStringReferenceArgArrayName(dimNameClause);

                            resolveDimension(inputSchemas, dimName, dimAlias, inputNo, dimNo, newParsingContext(dimNameClause), true);

                            if (variableWindow)
                            {
                                LOG4CXX_TRACE(logger, "This is variable_window so append dimension name");
                                shared_ptr<OperatorParamReference> refParam = make_shared<OperatorParamDimensionReference>(
                                                        newParsingContext(dimNameClause),
                                                        dimAlias,
                                                        dimName,
                                                        true);
                                resolveParamDimensionReference(preEvalInputSchemas, refParam);
                                windowParams.push_back(refParam);
                            }


                            if (windowSizes[dimNo].first != NULL)
                            {
                                fail(QPROC(SCIDB_LE_MULTIPLE_DIMENSION_SPECIFICATION,dimNameClause));
                            }
                            else
                            {
                                LOG4CXX_TRACE(logger, "Append window sizes");
                                windowSizes[dimNo].first = dimensionRange->get(windowDimensionRangeArgPreceding);
                                windowSizes[dimNo].second = dimensionRange->get(windowDimensionRangeArgFollowing);
                            }
                        }

                        if (!variableWindow && ranges->getSize() < inputSchema.getDimensions().size())
                        {
                            fail(QPROC(SCIDB_LE_NOT_ENOUGH_DIMENSIONS_IN_SPECIFICATION,windowClause));
                        }

                        dimNo = 0;
                        Node* unboundSizeAst = NULL;
                        BOOST_FOREACH(pairOfNodes &wsize, windowSizes)
                        {
                            //For variable_window we use single dimension so skip other
                            if (!wsize.first)
                                continue;
                            if (wsize.first->getInteger() < 0)
                            {
                                unboundSizeAst = _fac.newInteger(wsize.first->getWhere(),inputSchema.getDimensions()[dimNo].getLength());
                            }

                            windowParams.push_back(
                                    make_shared<OperatorParamLogicalExpression>(
                                        newParsingContext(wsize.first),
                                        AstToLogicalExpression(unboundSizeAst ? unboundSizeAst : wsize.first),
                                        TypeLibrary::getType(TID_VOID)));

                            if (unboundSizeAst)
                            {
                                unboundSizeAst = NULL;
                            }

                            if (wsize.second->getInteger() < 0)
                            {
                                unboundSizeAst = _fac.newInteger(wsize.second->getWhere(),
                                    inputSchema.getDimensions()[dimNo].getLength());
                            }

                            windowParams.push_back(
                                    make_shared<OperatorParamLogicalExpression>(
                                        newParsingContext(wsize.second),
                                        AstToLogicalExpression(unboundSizeAst ? unboundSizeAst : wsize.second),
                                        TypeLibrary::getType(TID_VOID)));

                            if (unboundSizeAst)
                            {
                                unboundSizeAst = NULL;
                            }
                        }

                        const string windowName = getString(windowClause,windowClauseArgName);

                        LOG4CXX_TRACE(logger, "Window name is: " << windowName);
                        if (aggregateParams.find(windowName) != aggregateParams.end())
                        {
                            LOG4CXX_TRACE(logger, "Such name already used. Halt.");
                            fail(QPROC(SCIDB_LE_PARTITION_NAME_NOT_UNIQUE,windowClause->get(windowClauseArgName)));
                        }

                        aggregateParams[windowName] = make_pair(variableWindow ? "variable_window" : "window", windowParams);
                    }

                    LOG4CXX_TRACE(logger, "Done with windows list");
                    break;
                }

                case regridClause:
                {
                    LOG4CXX_TRACE(logger, "Translating regrid");
                    const Node* regridDimensionsAST = grwAsClause->get(regridClauseArgDimensionsList);
                    vector<Node*> regridSizes(inputSchema.getDimensions().size(), NULL);

                    size_t inputNo;
                    size_t dimNo;

                    LOG4CXX_TRACE(logger, "Translating dimensions of window");
                    BOOST_FOREACH(const Node* regridDimension, regridDimensionsAST->getList())
                    {
                        Node* dimNameClause = regridDimension->get(regridDimensionArgName);

                        resolveDimension(inputSchemas,
                                         getStringReferenceArgName(dimNameClause),
                                         getStringReferenceArgArrayName(dimNameClause),
                                         inputNo,
                                         dimNo,
                                         newParsingContext(dimNameClause),
                                         true);

                        if (regridSizes[dimNo] != NULL)
                        {
                            fail(QPROC(SCIDB_LE_MULTIPLE_DIMENSION_SPECIFICATION,regridDimension));
                        }
                        else
                        {
                            regridSizes[dimNo] = regridDimension->get(regridDimensionArgStep);
                        }
                    }

                    if (regridDimensionsAST->getSize() != preEvalInputSchemas[0].getDimensions().size())
                    {
                        fail(SYNTAX(SCIDB_LE_WRONG_REGRID_REDIMENSION_SIZES_COUNT,regridDimensionsAST));
                    }

                    LogicalOperator::Parameters regridParams;

                    BOOST_FOREACH(Node *size, regridSizes)
                    {
                        regridParams.push_back(
                                make_shared<OperatorParamLogicalExpression>(
                                    newParsingContext(size),
                                    AstToLogicalExpression(size),
                                    TypeLibrary::getType(TID_VOID)));
                    }

                    aggregateParams[""] = make_pair("regrid", regridParams);
                    break;
                }
                case groupByClause:
                {
                    aggregateParams[""] = make_pair("aggregate", LogicalOperator::Parameters());
                    break;
                }
                case redimensionClause:
                {
                    LOG4CXX_TRACE(logger, "Adding schema to REDIMENSION parameters");

                    //First we iterate over all aggregates and prepare attributes for schema
                    //which will be inserted to redimension. We need to extract type of attribute
                    //from previous schema, which used in aggregate to get output type
                    set<string> usedNames;
                    Attributes redimensionAttrs;
                    BOOST_FOREACH(Node *aggCallNode, aggregateFunctions)
                    {
                        const chars aggName  = getStringApplicationArgName(aggCallNode);
                        const chars aggAlias = getString(aggCallNode,applicationArgAlias);

                        Type aggParamType;
                        if (aggCallNode->get(applicationArgOperands)->get(listArg0)->is(asterisk))
                        {
                            LOG4CXX_TRACE(logger, "Getting type of " << aggName << "(*) as " << aggAlias);
                            aggParamType = TypeLibrary::getType(TID_VOID);
                        }
                        else
                        if (aggCallNode->get(applicationArgOperands)->get(listArg0)->is(reference))
                        {
                            const chars aggAttrName = getStringReferenceArgName(aggCallNode->get(applicationArgOperands)->get(listArg0));

                            LOG4CXX_TRACE(logger, "Getting type of " << aggName << "(" << aggAttrName << ") as " << aggAlias);

                            BOOST_FOREACH(const AttributeDesc &attr, preEvalInputSchemas[0].getAttributes())
                            {
                                if (attr.getName() == aggAttrName)
                                {
                                    aggParamType = TypeLibrary::getType(attr.getType());
                                    break;
                                }
                            }
                        }
                        else
                        {
                            SCIDB_UNREACHABLE();
                        }

                        redimensionAttrs.push_back(AttributeDesc(redimensionAttrs.size(),
                                                                 aggAlias,
                                                                 AggregateLibrary::getInstance()->createAggregate(aggName, aggParamType)->getResultType().typeId(),
                                                                 AttributeDesc::IS_NULLABLE,
                                                                 0));
                        usedNames.insert(aggAlias);
                    }
                    redimensionAttrs.push_back(AttributeDesc(
                            redimensionAttrs.size(), DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                            TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));

                    //Now prepare dimensions
                    Dimensions redimensionDims;
                    passDimensions(grwAsClause->get(listArg0), redimensionDims, "", usedNames);

                    //Ok. Adding schema parameter
                    ArrayDesc redimensionSchema = ArrayDesc("", redimensionAttrs, redimensionDims, 0);
                    LOG4CXX_TRACE(logger, "Schema for redimension " <<  redimensionSchema);
                    aggregateParams[""] = make_pair("redimension",
                        LogicalOperator::Parameters(1,
                            make_shared<OperatorParamSchema>(newParsingContext(grwAsClause),redimensionSchema)));

                    break;
                }
                default: SCIDB_UNREACHABLE();
            }
        }
        else
        {
            //No additional parameters to aggregating operators
            aggregateParams[""] = make_pair("aggregate", LogicalOperator::Parameters());
        }

        BOOST_FOREACH(Node *aggCallNode, aggregateFunctions)
        {
            LOG4CXX_TRACE(logger, "Translating aggregate into logical aggregate call");
            if (aggCallNode->is(application))
            {
                if (isWindowClauses && grwAsClause->getSize() > 1)
                {
                    fail(QPROC(SCIDB_LE_PARTITION_NAME_NOT_SPECIFIED,aggCallNode));
                }

                aggregateParams.begin()->second.second.push_back(passAggregateCall(aggCallNode, preEvalInputSchemas));
            }
            else
            if (aggCallNode->is(olapAggregate))
            {
                const string partitionName = getString(aggCallNode,olapAggregateArgPartitionName);
                if (aggregateParams.end() == aggregateParams.find(partitionName))
                {
                    fail(QPROC(SCIDB_LE_UNKNOWN_PARTITION_NAME,aggCallNode->get(olapAggregateArgPartitionName)));
                }

                aggregateParams[partitionName].second.push_back(passAggregateCall(aggCallNode->get(olapAggregateArgApplication), preEvalInputSchemas));
            }
            else
            {
                SCIDB_UNREACHABLE();
            }
        }

        if (grwAsClause && grwAsClause->is(groupByClause))
        {
            BOOST_FOREACH(const Node *groupByItem,grwAsClause->getList(groupByClauseArgList))
            {
                assert(groupByItem->is(reference));

                if (groupByItem->has(referenceArgVersion))
                {
                    fail(SYNTAX(SCIDB_LE_REFERENCE_EXPECTED,groupByItem));
                }

                shared_ptr<OperatorParamReference> refParam = make_shared<OperatorParamDimensionReference>(
                                        newParsingContext(getReferenceArgName(groupByItem)),
                                        getStringReferenceArgArrayName(groupByItem),
                                        getStringReferenceArgName(groupByItem),
                                        true);
                resolveParamDimensionReference(preEvalInputSchemas, refParam);
                aggregateParams[""].second.push_back(refParam);
                break;
            }
        }

        LOG4CXX_TRACE(logger, "AGGREGATE/REGRID/WINDOW node appended");

        map<string,pair<string,LogicalOperator::Parameters> >::const_iterator it = aggregateParams.begin();

        LQPNPtr left = appendOperator(result, it->second.first,it->second.second, newParsingContext(selectList));

        ++it;

        for (;it != aggregateParams.end(); ++it)
        {
            LQPNPtr right = appendOperator(result, it->second.first,it->second.second, newParsingContext(selectList));

            LQPNPtr node = make_shared<LogicalQueryPlanNode>(
                newParsingContext(selectList), OperatorLibrary::getInstance()->createLogicalOperator("join"));
            node->addChild(left);
            node->addChild(right);
            left = node;
        }

        result = left;
    }

    if (joinOrigin)
    {
        LQPNPtr node = make_shared<LogicalQueryPlanNode>(
            newParsingContext(selectList),
            OperatorLibrary::getInstance()->createLogicalOperator("join"));
            node->addChild(result);
            node->addChild(input);
        result = node;
    }

    const vector<ArrayDesc> aggInputSchemas(1, result->inferTypes(_qry));

    if (postAggregationEvals.size())
    {
        LogicalOperator::Parameters applyParams;
        // Finally pass all post-aggregate evaluations and translate it into APPLY operators
        LOG4CXX_TRACE(logger, "Translating postAggregateEval into logical operator APPLY");
        BOOST_FOREACH(const Node *namedExprNode, postAggregationEvals)
        {
            assert(namedExprNode->is(namedExpr));

            // This is user output. Final attribute name will be used from AS clause (it can be defined
            // by user in _qry or generated by us above)
            applyParams.push_back(make_shared<OperatorParamAttributeReference>(
                newParsingContext(namedExprNode->get(namedExprArgName)),
                "",
                getString(namedExprNode,namedExprArgName),
                false));

            // This is expression which will be used as APPLY expression
            LEPtr lExpr = AstToLogicalExpression(namedExprNode->get(namedExprArgExpr));
            checkLogicalExpression(aggInputSchemas, ArrayDesc(), lExpr);
            shared_ptr<OperatorParam> exprParam = make_shared<OperatorParamLogicalExpression>(
                newParsingContext(namedExprNode->get(namedExprArgExpr)),
                lExpr, TypeLibrary::getType(TID_VOID));

            applyParams.push_back(exprParam);
        }

        result = appendOperator(result, "apply", applyParams, newParsingContext(selectList));
    }

    const vector<ArrayDesc> postEvalInputSchemas(1, result->inferTypes(_qry));

    if (projectParams.size() > 0)
    {
        BOOST_FOREACH(shared_ptr<OperatorParam> &param, projectParams)
        {
            resolveParamAttributeReference(postEvalInputSchemas, (shared_ptr<OperatorParamReference>&) param);
        }

        result = appendOperator(result, "project", projectParams, newParsingContext(selectList));
    }

    return result;
}

string Translator::genUniqueObjectName(const string& prefix, unsigned int &initialCounter,const vector<ArrayDesc> &inputSchemas, bool internal, cnodes namedExpressions)
{
    string name;

    while(true)
    {
        nextName:

        if (initialCounter == 0)
        {
            name = str(format("%s%s%s")
                % (internal ? "$" : "")
                % prefix
                % (internal ? "$" : "")
                );
            ++initialCounter;
        }
        else
        {
            name = str(format("%s%s_%d%s")
                % (internal ? "$" : "")
                % prefix
                % initialCounter
                % (internal ? "$" : "")
                );
            ++initialCounter;
        }

        BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
        {
            BOOST_FOREACH(const AttributeDesc& att, schema.getAttributes())
            {
                if (att.getName() == name)
                    goto nextName;
            }

            BOOST_FOREACH(const DimensionDesc dim, schema.getDimensions())
            {
                if (dim.hasNameAndAlias(name, ""))
                    goto nextName;
            }

            BOOST_FOREACH(const Node* ast, namedExpressions)
            {
                if (ast->is(namedExpr) && getString(ast,namedExprArgName) == name)
                    goto nextName;
            }
        }

        break;
    }

    return name;
}

LQPNPtr Translator::passThinClause(const Node *ast)
{
    LOG4CXX_TRACE(logger, "Translating THIN clause");
    typedef pair<Node*,Node*>   PairOfNodes;

    const Node* arrayRef = ast->get(thinClauseArgArrayReference);

    LQPNPtr result = AstToLogicalPlan(arrayRef);

    prohibitDdl    (result);
    prohibitNesting(result);

    const ArrayDesc& thinInputSchema = result->inferTypes(_qry);

    vector<PairOfNodes> thinStartStepList(thinInputSchema.getDimensions().size());

    size_t inputNo;
    size_t dimNo;

    BOOST_FOREACH(PairOfNodes& startStep, thinStartStepList)
    {
        startStep.first = startStep.second = NULL;
    }

    LOG4CXX_TRACE(logger, "Translating THIN start-step pairs");
    BOOST_FOREACH(const Node* thinDimension, ast->getList(thinClauseArgDimensionsList))
    {
        const Node* dimNameClause = thinDimension->get(thinDimensionClauseArgName);

        resolveDimension(
                vector<ArrayDesc>(1, thinInputSchema),
                getStringReferenceArgName(dimNameClause),
                getStringReferenceArgArrayName(dimNameClause),
                inputNo,
                dimNo,
                newParsingContext(dimNameClause),
                true);

        if (thinStartStepList[dimNo].first != NULL)
        {
            fail(QPROC(SCIDB_LE_MULTIPLE_DIMENSION_SPECIFICATION,dimNameClause));
        }
        else
        {
            thinStartStepList[dimNo].first = thinDimension->get(thinDimensionClauseArgStart);
            thinStartStepList[dimNo].second = thinDimension->get(thinDimensionClauseArgStep);
        }
    }

    if (ast->get(thinClauseArgDimensionsList)->getSize() < thinInputSchema.getDimensions().size())
    {
        fail(QPROC(SCIDB_LE_NOT_ENOUGH_DIMENSIONS_IN_SPECIFICATION,ast->get(thinClauseArgDimensionsList)));
    }

    LogicalOperator::Parameters thinParams;
    BOOST_FOREACH(PairOfNodes &startStep, thinStartStepList)
    {
        thinParams.push_back(
            make_shared<OperatorParamLogicalExpression>(
                newParsingContext(startStep.first),
                AstToLogicalExpression(startStep.first),
                TypeLibrary::getType(TID_VOID)));
        thinParams.push_back(
            make_shared<OperatorParamLogicalExpression>(
                newParsingContext(startStep.second),
                AstToLogicalExpression(startStep.second),
                TypeLibrary::getType(TID_VOID)));
    }

    result = appendOperator(result, "thin", thinParams, newParsingContext(ast));

    return result;
}

void Translator::prohibitDdl(const LQPNPtr& planNode)
{
    if (planNode->isDdl())
    {
        fail(QPROC(SCIDB_LE_DDL_CANT_BE_NESTED,planNode->getParsingContext()));
    }
}

void Translator::prohibitNesting(const LQPNPtr& planNode)
{
    if (planNode->getLogicalOperator()->getProperties().noNesting)
    {
        fail(QPROC(SCIDB_LE_NESTING_PROHIBITED,planNode->getParsingContext()) << planNode->getLogicalOperator()->getLogicalName());
    }
}

void Translator::passReference(const Node* ast,chars& alias,chars& name)
{
    if (ast->has(referenceArgVersion))
    {
        fail(SYNTAX(SCIDB_LE_REFERENCE_EXPECTED,ast->get(referenceArgVersion)));
    }

    if (ast->has(referenceArgOrder))
    {
        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
    }

    alias = getStringReferenceArgArrayName(ast);
    name  = getStringReferenceArgName(ast);
}

LQPNPtr Translator::fitInput(LQPNPtr &input,const ArrayDesc& destinationSchema)
{
    ArrayDesc inputSchema = input->inferTypes(_qry);
    LQPNPtr fittedInput = input;

    if (!inputSchema.getEmptyBitmapAttribute()
        && destinationSchema.getEmptyBitmapAttribute())
    {
        vector<shared_ptr<OperatorParam> > betweenParams;
        for (size_t i=0, n=destinationSchema.getDimensions().size(); i<n; ++i)
        {
            Value bval(TypeLibrary::getType(TID_INT64));
            bval.setNull();
            shared_ptr<OperatorParamLogicalExpression> param = make_shared<OperatorParamLogicalExpression>(
                input->getParsingContext(),
                make_shared<Constant>(input->getParsingContext(), bval, TID_INT64),
                TypeLibrary::getType(TID_INT64), true);
            betweenParams.push_back(param);
            betweenParams.push_back(param);
        }

        fittedInput = appendOperator(input, "between", betweenParams, input->getParsingContext());
        inputSchema = fittedInput->inferTypes(_qry);
    }

    bool needCast = false;
    bool needRepart = false;

    //Give up on casting if schema objects count differ. Nothing to do here.
    if (destinationSchema.getAttributes().size()
        == inputSchema.getAttributes().size()
        && destinationSchema.getDimensions().size()
            == inputSchema.getDimensions().size())
    {
        for (size_t attrNo = 0; attrNo < inputSchema.getAttributes().size();
            ++attrNo)
        {
            const AttributeDesc &inAttr =
                destinationSchema.getAttributes()[attrNo];
            const AttributeDesc &destAttr = inputSchema.getAttributes()[attrNo];

            //If attributes has differ names we need casting...
            if (inAttr.getName() != destAttr.getName())
                needCast = true;

            //... but if type and flags differ we can't cast
            if (inAttr.getType() != destAttr.getType()
                || inAttr.getFlags() != destAttr.getFlags())
            {
                needCast = false;
                goto noCastAndRepart;
            }
        }

        for (size_t dimNo = 0; dimNo < inputSchema.getDimensions().size();
            ++dimNo)
        {
            const DimensionDesc &destDim =destinationSchema.getDimensions()[dimNo];
            const DimensionDesc &inDim = inputSchema.getDimensions()[dimNo];

            //If dimension has differ names we need casting...
            if (inDim.getBaseName() != destDim.getBaseName())
                needCast = true;

            //If dimension has different chunk size we need repart..
            if (inDim.getChunkOverlap() != destDim.getChunkOverlap()
             || inDim.getChunkInterval() != destDim.getChunkInterval())
                needRepart = true;

            //... but if length or type of dimension differ we cant cast and repart
            if (inDim.getStartMin() != destDim.getStartMin()
                || !(inDim.getEndMax() == destDim.getEndMax()
                    || (inDim.getEndMax() < destDim.getEndMax()
                        && ((inDim.getLength() % inDim.getChunkInterval()) == 0
                            || inputSchema.getEmptyBitmapAttribute() != NULL))))
            {
                needCast = false;
                needRepart = false;
                goto noCastAndRepart;
            }
        }

    }

    noCastAndRepart:

    try
    {
        if (needRepart)
        {
            shared_ptr<LogicalOperator> repartOp;
            LOG4CXX_TRACE(logger, "Inserting REPART operator");
            repartOp = OperatorLibrary::getInstance()->createLogicalOperator("repart");

            LogicalOperator::Parameters repartParams(1,
                make_shared<OperatorParamSchema>(input->getParsingContext(),destinationSchema));
            repartOp->setParameters(repartParams);

            LQPNPtr tmpNode = make_shared<LogicalQueryPlanNode>(input->getParsingContext(), repartOp);
            tmpNode->addChild(fittedInput);
            tmpNode->inferTypes(_qry);
            fittedInput = tmpNode;
        }

        if (needCast)
        {
            shared_ptr<LogicalOperator> castOp;
            LOG4CXX_TRACE(logger, "Inserting CAST operator");
            castOp = OperatorLibrary::getInstance()->createLogicalOperator("cast");

            LogicalOperator::Parameters castParams(1,
                make_shared<OperatorParamSchema>(input->getParsingContext(),destinationSchema));
            castOp->setParameters(castParams);

            LQPNPtr tmpNode = make_shared<LogicalQueryPlanNode>(input->getParsingContext(), castOp);
            tmpNode->addChild(fittedInput);
            tmpNode->inferTypes(_qry);
            fittedInput = tmpNode;
        }
    }
    catch (const UserException& e)
    {
        if (SCIDB_SE_INFER_SCHEMA == e.getShortErrorCode())
        {
            LOG4CXX_TRACE(logger, "Can not infer schema from REPART and/or CAST. Give up.");
        }
        else
        {
            LOG4CXX_TRACE(logger, "Something going wrong");
            throw;
        }
    }

    return fittedInput;
}

LQPNPtr Translator::canonicalizeTypes(const LQPNPtr &input)
{
    LOG4CXX_TRACE(logger, "Types canonicalization");
    const ArrayDesc& inputSchema = input->inferTypes(_qry);
    bool skip = true;
    BOOST_FOREACH(const AttributeDesc& att, inputSchema.getAttributes())
    {
        if(!isBuiltinType(att.getType()))
        {
            skip = false;
            break;
        }
    }

    if (skip)
    {
        return input;
    }

    ContextPtr pc = input->getParsingContext();
    vector<shared_ptr<OperatorParam> > castParams(1);

    Attributes attrs;
    BOOST_FOREACH(const AttributeDesc& att, inputSchema.getAttributes())
    {
        TypeId attType;
        if(isBuiltinType(att.getType()))
        {
            attType = att.getType();
        }
        else
        {
            attType = TID_STRING;
        }
        AttributeDesc newAtt(
                att.getId(),
                att.getName(),
                attType,
                att.getFlags(),
                att.getDefaultCompressionMethod(),
                att.getAliases(),
                att.getReserve());
        attrs.push_back(newAtt);
    }

    ArrayDesc castSchema(
            inputSchema.getId(),
            inputSchema.getUAId(),
            inputSchema.getVersionId(),
            inputSchema.getName(),
            attrs,
            inputSchema.getDimensions(),
            inputSchema.getFlags());

    castParams[0] = make_shared<OperatorParamSchema>(pc, castSchema);

    return appendOperator(input, "cast", castParams, pc);
}

/****************************************************************************/
/* Expressions */

LEPtr Translator::AstToLogicalExpression(const Node* ast)
{
    switch (ast->getType())
    {
        case cnull:             return onNull(ast);
        case creal:             return onReal(ast);
        case cstring:           return onString(ast);
        case cboolean:          return onBoolean(ast);
        case cinteger:          return onInteger(ast);
        case application:       return onScalarFunction(ast);
        case reference:         return onAttributeReference(ast);
        case olapAggregate:     fail(SYNTAX(SCIDB_LE_WRONG_OVER_USAGE,ast));
        case asterisk:          fail(SYNTAX(SCIDB_LE_WRONG_ASTERISK_USAGE,ast));
        case selectArray:       fail(SYNTAX(SCIDB_LE_SUBQUERIES_NOT_SUPPORTED,ast));
        default:                fail(INTERNAL(SCIDB_LE_UNKNOWN_ERROR,ast) << ast->getType());
    }

    SCIDB_UNREACHABLE();
    return LEPtr();
}

LEPtr Translator::onNull(const Node* ast)
{
    assert(ast->is(cnull));

    Value c; c.setNull();

    return make_shared<Constant>(newParsingContext(ast),c,TID_VOID);
}

LEPtr Translator::onReal(const Node* ast)
{
    assert(ast->is(creal));

    Value c(TypeLibrary::getType(TID_DOUBLE));
    c.setDouble(ast->getReal());

    return make_shared<Constant>(newParsingContext(ast),c,TID_DOUBLE);
}

LEPtr Translator::onString(const Node* ast)
{
    assert(ast->is(cstring));

    Value c(TypeLibrary::getType(TID_STRING));
    c.setString(ast->getString());

    return make_shared<Constant>(newParsingContext(ast),c,TID_STRING);
}

LEPtr Translator::onBoolean(const Node* ast)
{
    assert(ast->is(cboolean));

    Value c(TypeLibrary::getType(TID_BOOL));
    c.setBool(ast->getBoolean());

    return make_shared<Constant>(newParsingContext(ast),c,TID_BOOL);
}

LEPtr Translator::onInteger(const Node* ast)
{
    assert(ast->is(cinteger));

    Value c(TypeLibrary::getType(TID_INT64));
    c.setInt64(ast->getInteger());

    return make_shared<Constant>(newParsingContext(ast),c,TID_INT64);
}

LEPtr Translator::onScalarFunction(const Node* ast)
{
    assert(ast->is(application));

    chars         name(getStringApplicationArgName(ast));
    vector<LEPtr> args;

    if (OperatorLibrary::getInstance()->hasLogicalOperator(name))
    {
        fail(SYNTAX(SCIDB_LE_UNEXPECTED_OPERATOR_IN_EXPRESSION,ast));
    }

    BOOST_FOREACH (const Node* a,ast->getList(applicationArgOperands))
    {
        args.push_back(AstToLogicalExpression(a));
    }

    return make_shared<Function>(newParsingContext(ast),name,args);
}

LEPtr Translator::onAttributeReference(const Node* ast)
{
    assert(ast->is(reference));

    if (ast->has(referenceArgVersion))
    {
        fail(SYNTAX(SCIDB_LE_REFERENCE_EXPECTED,ast->get(referenceArgVersion)));
    }

    if (ast->has(referenceArgOrder))
    {
        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
    }

    return make_shared<AttributeReference>(
            newParsingContext(ast),
            getStringReferenceArgArrayName(ast),
            getStringReferenceArgName(ast));
}

/****************************************************************************/

LEPtr translate(Factory& f,Log& l,const StringPtr& s,Node* n)
{
    return Translator(f,l,s).AstToLogicalExpression(n);
}

LQPNPtr translate(Factory& f,Log& l,const StringPtr& s,Node* n,const QueryPtr& q)
{
    return Translator(f,l,s,q).AstToLogicalPlan(n,true);
}

/****************************************************************************/
}}
/****************************************************************************/
