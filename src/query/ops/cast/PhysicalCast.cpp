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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/DelegateArray.h"
#include "CastArray.h"

using namespace std;
using namespace boost;

namespace scidb {


class PhysicalCast: public PhysicalOperator
{
public:
	PhysicalCast(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }


	/***
	 * Cast is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
        boost::shared_ptr<Array> inputArray = inputArrays[0];
        if (_schema.getAttributes().size() != inputArray->getArrayDesc().getAttributes().size()) 
        { 
            inputArray = boost::shared_ptr<Array>(new NonEmptyableArray(inputArray));
        }

        CastArray::CastingMap castingMap;

        for(AttributeID i = 0, n = _schema.getAttributes().size(); i < n; ++i)
        {
            TypeId from = inputArray->getArrayDesc().getAttributes()[i].getType();
            TypeId to = _schema.getAttributes()[i].getType();
            if (from != to)
            {
                castingMap[i] = FunctionLibrary::getInstance()->findConverter(
                        from, to);
            }
        }

        Dimensions dstDimensions(_schema.getDimensions().size());
        for (size_t i = 0, n = _schema.getDimensions().size(); i < n; i++)
        {
            const DimensionDesc &srcDim = _schema.getDimensions()[i];
            dstDimensions[i] = DimensionDesc(
                    srcDim.getBaseName(),
                    srcDim.getNamesAndAliases(),
                    srcDim.getStartMin(),
                    srcDim.getCurrStart(),
                    srcDim.getCurrEnd(),
                    srcDim.getEndMax(),
                    srcDim.getChunkInterval(),
                    srcDim.getChunkOverlap());
        }

        ArrayDesc dstSchema("", _schema.getAttributes(), dstDimensions, _schema.getFlags());

        return boost::shared_ptr<Array>(new CastArray(dstSchema, inputArray, castingMap));
	 }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCast, "cast", "physicalCast")

}  // namespace scidb
