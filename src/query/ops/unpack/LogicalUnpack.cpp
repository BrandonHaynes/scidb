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
 * LogicalUnpack.cpp
 *
 *  Created on: Mar 9, 2010
 *      Author: Emad
 *      Author: poliocough@gmail.com
 */

#include "query/Operator.h"
#include "system/Exceptions.h"

namespace scidb
{

using namespace std;

/***
 * Helper function to construct array descriptor for the unpacked array
 ***/
inline ArrayDesc addAttributes(ArrayDesc const& desc, string const& dimName, size_t const chunkSize)
{
    Attributes const& oldAttributes = desc.getAttributes();
    Dimensions const& dims = desc.getDimensions();
    Attributes newAttributes(oldAttributes.size() + dims.size()); 
    size_t i = 0;
    uint64_t arrayLength = 1;
    for (size_t j = 0; j < dims.size(); j++, i++)
    {
        arrayLength *= dims[j].getLength();
        newAttributes[i] = AttributeDesc(i, dims[j].getBaseName(), TID_INT64, 0, 0);
    }

    for (size_t j = 0; j < oldAttributes.size(); j++, i++)
    {
        AttributeDesc const& attr = oldAttributes[j];
        newAttributes[i] = AttributeDesc((AttributeID)i, attr.getName(), attr.getType(), attr.getFlags(),
            attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
            attr.getDefaultValueExpr());
    }

    newAttributes = addEmptyTagAttribute(newAttributes);

    Dimensions newDimensions(1);
    newDimensions[0] = DimensionDesc(dimName, 0, 0, MAX_COORDINATE, MAX_COORDINATE, chunkSize, 0);

    return ArrayDesc(desc.getName(), newAttributes, newDimensions);
}
    
/**
 * @brief The operator: unpack().
 *
 * @par Synopsis:
 *   unpack( srcArray, newDim )
 *
 * @par Summary:
 *   Unpacks a multi-dimensional array into a single-dimensional array,
 *   creating new attributes to represent the dimensions in the source array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - newDim: the name of the dimension in the result 1D array.
 *
 * @par Output array:
 *        <
 *   <br>   srcDims (as attributes in the output), followed by srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   newDim: start=0, end=#logical cells in srcArray less 1, chunk interval=the chunk interval of the last dimension in srcDims
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
class LogicalUnpack: public LogicalOperator
{
public:
    LogicalUnpack(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_OUT_DIMENSION_NAME()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(PARAM_CONSTANT("int64"));
        res.push_back(END_OF_VARIES_PARAMS());
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getParamType() == PARAM_DIMENSION_REF);
        const string &dimName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        size_t chunkSize = 0;
        if(_parameters.size() == 2)
        {
            chunkSize = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(), query, TID_INT64).getInt64();
            if(chunkSize <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
            }
        }

        size_t inputSchemaSize = schemas[0].getSize();
        if (chunkSize == 0) //not set by user
        {
            //1M is a good/recommended chunk size for most one-dimensional arrays -- unless you are using
            //large strings or UDTs
            chunkSize = 1000000;

            //If there's no way that the input has one million elements - reduce the chunk size further.
            //This is ONLY done for aesthetic purposes - don't want to see one million "()" on the screen.
            //In fact, sometimes it can become a liability...
            if(inputSchemaSize<chunkSize)
            {
                //make chunk size at least 1 to avoid errors
                chunkSize = std::max<size_t>(inputSchemaSize,1);
            }
        }

        return addAttributes(schemas[0], dimName, chunkSize);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalUnpack, "unpack")

} //namespace
