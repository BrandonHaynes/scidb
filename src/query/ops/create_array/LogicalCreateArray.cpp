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

/**
 * @file
 *
 * @brief Logical DDL operator that creates a new persistent array.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include "query/Operator.h"

namespace scidb
{

/**
 * @brief Implements the create_array() operator.
 *
 * @par Synopsis:
 *
 *  @code
 *      create_array ( array_name, array_schema [,'TEMP'] )
 *  @endcode
 *  or
 *  @code
 *      CREATE ['TEMP'] ARRAY array_name  array_schema
 *  @endcode
 *
 * @par Summary:
 *      Creates an array with the given name and schema and adds it to the database.
 *
 * @par Input:
 *      - array_name:      an identifier that names the new array.
 *      - array_schema:    a multidimensional array schema that describes the rank
 *                          and shape of the array to be created, as well as the types
 *                          of each its attributes.
 *
 *  An array schema has the following form:
 *
 *  @code
 *    array_schema := '<' attributes '>' '[' dimensions ']'
 *
 *    attributes   := attribute {',' attribute}*
 *
 *    dimensions   := dimension {',' dimension}*
 *
 *    attribute    := attribute_name ':' type [[NOT] NULL] [DEFAULT default_value] [COMPRESSION compression_algorithm] [RESERVE integer]
 *
 *    dimension    := dimension_name [= dim_low ':' {dim_high|'*'} ',' chunk_interval ',' chunk_overlap]
 *  @endcode
 *
 *  Note:
 *    - For a list of attribute types, use list('types'). Note that loading a plugin may introduce more types.
 *
 *    - The optional constraints of an attribute have the following meaning and default values:
 *      <ul>
 *
 *      <li>[[NOT] NULL]:            indicates whether the attribute may contain null values.
 *                                   Note that SciDB supports multiple null values, with different 'missing' reason codes.
 *                                   You may specify a null value with the function missing(i), where 0<=i<=127.
 *                                   Default is NOT NULL, meaning null values are not allowed.
 *
 *      <li>[DEFAULT default_value]: the value to be automatically substituted when a non-NULL attribute lacks a value.
 *                                   If the attribute is declared as NULL, this clause is ignored, and 'null' (with missing reason 0)
 *                                   will be used as the default value.
 *                                   Otherwise, the default value is 0 for numeric types and "" for the string type.
 *
 *      <li>[COMPRESSION string]:    the compression algorithm that is used to compress chunk data before storing on disk.
 *                                   Default is 'no compression'.
 *
 *                                   Paul describes COMPRESSION it thus:
 *
 *                                   " a place holder that will allow users or administrators to tell SciDB which general
 *                                   compression method—for example, gzip—they want to apply to chunks for this attribute.
 *                                   The range of legal values is open: the idea was to make the list of compression methods
 *                                   extensible. The default is no compression. "
 *
 *                                   The syntax is currently recognized and is stored in the meta data, but does nothing
 *                                   at the moment.
 *
 *                                   This should not be described in the user manual.
 *
 *      <li>[RESERVE integer]:       the value of the CONFIG_CHUNK_RESERVE config file setting
 *
 *                                   Paul describes RESERVE it thus:
 *
 *                                   " RESERVE is used to reserve space at the end of a chunk for delta compression. If we
 *                                    anticipate that the update rate for this particular attribute is likely to be high,
 *                                    we’ll reserve more space at the end of the chunk. Because the deltas are turned off,
 *                                    this currently is inoperative."
 *
 *                                   This should not be described in the user manual.
 *      </ul>
 *
 *    - array_name, attribute_name, dimension_name are all identifiers
 *
 *    - dim_low, dim_high, chunk_interval, and chunk_overlap are expressions that should evaluate to a 64-bit integer.
 *
 * @par Output array:
 *        <
 *   <br>   attributes
 *   <br> >
 *   <br> [
 *   <br>   dimensions
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalCreateArray: public LogicalOperator
{
public:
    LogicalCreateArray(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.ddl = true;
        ADD_PARAM_OUT_ARRAY_NAME()
        ADD_PARAM_SCHEMA()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > v(2);
        v[0] = END_OF_VARIES_PARAMS();
        v[1] = PARAM_CONSTANT(TID_STRING);
        return v;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() >= 2);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        assert(_parameters[1]->getParamType() == PARAM_SCHEMA);

        const string &name = ((boost::shared_ptr<OperatorParamArrayReference>&)_parameters[0])->getObjectName();

        if (SystemCatalog::getInstance()->containsArray(name))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_ALREADY_EXIST,
                _parameters[0]->getParsingContext()) << name;
        }

        if (_parameters.size() >= 3)
        {
            std::string s(evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),query,TID_STRING).getString());

            if (compareStringsIgnoreCase(s,"temp") != 0)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,SCIDB_LE_UNSUPPORTED_FORMAT,_parameters[2]->getParsingContext()) << s;
            }
        }

        return ArrayDesc();
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() > 0);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);

        const string& arrayName = ((boost::shared_ptr<OperatorParamArrayReference>&)_parameters[0])->getObjectName();
        assert(!arrayName.empty());
        assert(arrayName.find('@') == std::string::npos);
        boost::shared_ptr<SystemCatalog::LockDesc> lock(new SystemCatalog::LockDesc(arrayName,
                                                                                    query->getQueryID(),
                                                                                    Cluster::getInstance()->getLocalInstanceId(),
                                                                                    SystemCatalog::LockDesc::COORD,
                                                                                    SystemCatalog::LockDesc::CRT));
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::CRT);
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCreateArray, "create_array")

} //namespace
