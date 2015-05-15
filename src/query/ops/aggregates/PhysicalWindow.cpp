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
 * PhysicalWindow.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik, poliocough@gmail.com, 
 *              Paul Brown <paulgeoffreybrown@gmail.com>
 */
#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "WindowArray.h"

namespace scidb {

using namespace boost;
using namespace std;

class PhysicalWindow: public  PhysicalOperator
{
private:
    vector<WindowBoundaries> _window;

public:

    PhysicalWindow(const string& logicalName, const string& physicalName,
                   const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        size_t nDims = _schema.getDimensions().size();
        _window = vector<WindowBoundaries>(nDims);
        for (size_t i = 0, size = nDims * 2, boundaryNo = 0; i < size; i+=2, ++boundaryNo)
        {
            _window[boundaryNo] = WindowBoundaries(
                    ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt64(),
                    ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i+1])->getExpression()->evaluate().getInt64()
                    );
        }
    }

    /**
     * @see PhysicalOperator::requiresRepart()
     */
    virtual void requiresRepart(vector<ArrayDesc> const& inputSchemas,
                                vector<ArrayDesc const*>& repartPtrs) const
    {
        assert(inputSchemas.size() == 1);
        assert(repartPtrs.size() == 1);
        ArrayDesc const& inputSchema = inputSchemas[0];

        Dimensions const& dims = inputSchema.getDimensions();
        for (size_t i = 0; i < dims.size(); i++)
        {
            DimensionDesc const& srcDim = dims[i];
            if(static_cast<uint64_t>(srcDim.getChunkInterval()) != srcDim.getLength() &&
               srcDim.getChunkOverlap() < std::max(_window[i]._boundaries.first, _window[i]._boundaries.second))
            {
                repartPtrs[0] = getRepartSchema(inputSchema);
                return;
            }
        }
        repartPtrs.clear();
    }

    ArrayDesc* getRepartSchema(ArrayDesc const& inputSchema) const
    {
        _repartSchemas.clear(); // Forget previous results, if any.
        Attributes attrs = inputSchema.getAttributes();

        Dimensions dims;
        for (size_t i =0; i<inputSchema.getDimensions().size(); i++)
        {
            DimensionDesc inDim = inputSchema.getDimensions()[i];

            int64_t overlap = inDim.getChunkOverlap();
            int64_t const neededOverlap = std::max(_window[i]._boundaries.first, _window[i]._boundaries.second);
            if ( neededOverlap > inDim.getChunkOverlap())
            {
                overlap = neededOverlap;
            }

            dims.push_back( DimensionDesc(inDim.getBaseName(),
                                          inDim.getNamesAndAliases(),
                                          inDim.getStartMin(),
                                          inDim.getCurrStart(),
                                          inDim.getCurrEnd(),
                                          inDim.getEndMax(),
                                          inDim.getChunkInterval(),
                                          overlap));
        }

        _repartSchemas.push_back(make_shared<ArrayDesc>(inputSchema.getName(), attrs, dims));
        return _repartSchemas.back().get();
    }

    void verifyInputSchema(ArrayDesc const& input) const
    {
        Dimensions const& dims = input.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = dims[i];
            if (static_cast<uint64_t>(srcDim.getChunkInterval()) != srcDim.getLength() &&
                srcDim.getChunkOverlap() < std::max(_window[i]._boundaries.first, _window[i]._boundaries.second))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_WINDOW_ERROR2);
        }
    }

    /**
     * window(...) is a pipelined operator, hence it executes by returning an 
     * iterator-based array to the consumer that overrides the chunkiterator 
     * method.
     */
    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(inputArrays.size() == 1);

        shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
        ArrayDesc const& inDesc = inputArray->getArrayDesc();
        verifyInputSchema(inDesc);

        vector<AttributeID> inputAttrIDs;
        vector<AggregatePtr> aggregates;
        string method;

        //
        //  Probe the list of operator parameters for aggregates and 
        // the optional "method" argument. Note that checks about the 
        // correctness of these arguments (valid aggregate names, 
        // valid method names) have already occurred in LogicalWindow. 
        for (size_t i = inDesc.getDimensions().size() * 2, size = _parameters.size(); i < size; i++)
        {
            boost::shared_ptr<scidb::OperatorParam> & param = _parameters[i];
             
            if (PARAM_AGGREGATE_CALL == param->getParamType()) 
            { 
                AttributeID inAttId;
                AggregatePtr agg = resolveAggregate((shared_ptr <OperatorParamAggregateCall> const&) _parameters[i],
                                                inDesc.getAttributes(),
                                                &inAttId,
                                                0);

                aggregates.push_back(agg);

                if (inAttId == (AttributeID) -1) 
                {
                    //for count(*); optimize later
                    inputAttrIDs.push_back(0);
                } else 
                {
                    inputAttrIDs.push_back(inAttId);
                }
            } else if (PARAM_PHYSICAL_EXPRESSION == param->getParamType()) 
            { 
                method = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)param)->getExpression()->evaluate().getString();
            }
        }
 
        return boost::shared_ptr<Array>(new WindowArray(_schema, inputArray, _window, inputAttrIDs, aggregates, method));
    }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalWindow, "window", "physicalWindow")

}  // namespace scidb
