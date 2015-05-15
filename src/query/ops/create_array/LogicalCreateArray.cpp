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

#define fail(e) throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,e)

namespace scidb {

using namespace std;
using boost::shared_ptr;
/**
 * @brief Implements the create_array() operator.
 *
 * @par Synopsis:
 *
 *  @code
 *      create_array ( array_name, array_schema , temp [, load_array , cells ] )
 *  @endcode
 *  or
 *  @code
 *      CREATE ['TEMP'] ARRAY array_name  array_schema [ [ [cells] ] USING load_array ]
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
 *      - temp:            a boolean flag, true for a temporary array, false for a db array.
 *      - load_array       an existing database array whose values are to be used to determine
 *                         sensible choices for those details of the target dimensions that
 *                         were elided.
 *      - cells            the desired number of logical cells per chunk (default is 1M)
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
 *    dimension    := dimension_name [= {dim_low|'?'} ':' {dim_high|'?'|'*'} ',' {chunk_interval|'?'} ',' {chunk_overlap|'?'}]
 *  @endcode
 *
 *  where a '?' in place of a dimension detail indicates the a sensible default value should be supplied by the system.
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
 *                                   Paul describes COMPRESSION thus:
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
struct LogicalCreateArray : LogicalOperator
{
    LogicalCreateArray(const string& logicalName,const string& alias,bool as = false)
     : LogicalOperator(logicalName,alias)
    {
        if (as)                                          // Create_Array_AS?
        {
            ADD_PARAM_INPUT()                            // The dimension matrix
            ADD_PARAM_INPUT()                            // The distinct count
            _properties.noNesting = true;
        }
        else
        {
            _properties.ddl       = true;
        }

        ADD_PARAM_OUT_ARRAY_NAME()                       // The array name
        ADD_PARAM_SCHEMA()                               // The array schema
        ADD_PARAM_CONSTANT(TID_BOOL);                    // The temporary flag
    }

    ArrayDesc inferSchema(vector<ArrayDesc>,shared_ptr<Query> query)
    {
        assert(param<OperatorParam>(0)->getParamType() == PARAM_ARRAY_REF);
        assert(param<OperatorParam>(1)->getParamType() == PARAM_SCHEMA);

        string n(param<OperatorParamArrayReference>(0)->getObjectName());

        if (SystemCatalog::getInstance()->containsArray(n))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_ALREADY_EXIST,_parameters[0]->getParsingContext()) << n;
        }

        return ArrayDesc();
    }

    void inferArrayAccess(shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);

        assert(param<OperatorParam>(0)->getParamType() == PARAM_ARRAY_REF);

        string n(param<OperatorParamArrayReference>(0)->getObjectName());

        assert(!n.empty() && n.find('@')==string::npos);  // no version number

        shared_ptr<SystemCatalog::LockDesc> lock(make_shared<SystemCatalog::LockDesc>(n,
                                                                                    query->getQueryID(),
                                                                                    Cluster::getInstance()->getLocalInstanceId(),
                                                                                    SystemCatalog::LockDesc::COORD,
                                                                                    SystemCatalog::LockDesc::CRT));
        shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::CRT);
    }

    template<class t>
    shared_ptr<t>& param(size_t i) const
    {
        assert(i < _parameters.size());

        return (shared_ptr<t>&)_parameters[i];
    }
};

/**
 *  @brief Implements the create_array_as() operator, an undocumented variant
 *  of the create_array operator that implements automatic chunk size selection.
 *  Exactly the same as create_array(), but takes two initial array arguments
 *  into which the statistics of a 'load array' are first computed.
 */
struct LogicalCreateArrayUsing : LogicalCreateArray
{
    LogicalCreateArrayUsing(const string& logicalName,const string& alias)
     : LogicalCreateArray(logicalName,alias,true)       // 2 axtra arrays at front
    {}

    ArrayDesc inferSchema(vector<ArrayDesc> schema,shared_ptr<Query> query)
    {
        assert(schema.size() == 2);

    /** First call the inherited method to provide the default validation, but
        no need to save the result because we know what it will be: like every
        DDL command, we just return an empty array...*/

        LogicalCreateArray::inferSchema(schema,query);

        if (schema[0].getDimensions().size() != 1)
        {
            fail(SCIDB_LE_UNKNOWN_ERROR) << "expecting exactly one dimension";
        }

        if (schema[0].getDimensions()[0].getLength() !=
           param<OperatorParamSchema>(1)->getSchema().getDimensions().size())
        {
            fail(SCIDB_LE_UNKNOWN_ERROR) << "bad array length";
        }

        if (schema[0].getAttributes().size() < 7)
        {
            fail(SCIDB_LE_UNKNOWN_ERROR) << "too few attributes";
        }


        for (size_t i=0; i!=schema[0].getAttributes(true).size() ; ++i)
        {
            if (schema[0].getAttributes()[i].getType() != TID_INT64)
            {
                fail(SCIDB_LE_UNKNOWN_ERROR) << "int64 attribute expected";
            }
        }

        if (schema[1].getAttributes()[0].getType() != TID_UINT64)
        {
            fail(SCIDB_LE_UNKNOWN_ERROR) << "uint64 attribute expected";
        }

    /** DLL commands are non-nestable, and always return a null array...*/

        return ArrayDesc();
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCreateArray,     "create_array")
DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCreateArrayUsing,"create_array_using")

} //namespace
