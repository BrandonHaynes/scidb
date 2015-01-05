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
 * LogicalAggregate.cpp
 *
 *  Created on: Jul 7, 2011
 *      Author: poliocough@gmail.com
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"
#include "query/TypeSystem.h"
#include "query/Aggregate.h"
#include "util/arena/Set.h"

namespace scidb {

using namespace std;
using namespace boost;

/**
 * @brief The operator: aggregate().
 *
 * @par Synopsis:
 *   aggregate( srcArray {, AGGREGATE_CALL}+ {, groupbyDim}* )
 *   <br> AGGREGATE_CALL := AGGREGATE_FUNC(inputAttr) [as resultName]
 *   <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *
 * @par Summary:
 *   Calculates aggregates over groups of values in an array, given the aggregate types and attributes to aggregate on. <br>
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 1 or more aggregate calls.
 *     Each aggregate call has an AGGREGATE_FUNC, an inputAttr and a resultName.
 *     The default resultName is inputAttr followed by '_' and then AGGREGATE_FUNC.
 *     For instance, the default resultName for sum(sales) is 'sales_sum'.
 *     The count aggregate may take * as the input attribute, meaning to count all the items in the group including null items.
 *     The default resultName for count(*) is 'count'.
 *   - 0 or more dimensions that together determines the grouping criteria.
 *
 * @par Output array:
 *        <
 *   <br>   The aggregate calls' resultNames.
 *   <br> >
 *   <br> [
 *   <br>   The list of groupbyDims, if exists; or
 *   <br>   i, if no groupbyDim is provided.
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *   - aggregate(A, count(*), max(quantity), sum(sales), year) <count: uint64, quantity_max: uint64, sales_sum: double> [year] =
 *     <br> year, count, quantity_max, sales_sum
 *     <br> 2011,   2,      7,           51.62
 *     <br> 2012,   3,      9,          108.97
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - All the aggregate functions ignore null values, except count(*).
 *
 */
class LogicalAggregate: public  LogicalOperator
{
public:
    LogicalAggregate(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 0)
        {
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        else
        {
            boost::shared_ptr<OperatorParam> lastParam = _parameters[_parameters.size() - 1];
            if (lastParam->getParamType() == PARAM_AGGREGATE_CALL)
            {
                res.push_back(PARAM_AGGREGATE_CALL());
                res.push_back(PARAM_IN_DIMENSION_NAME());
            }
            else if (lastParam->getParamType() == PARAM_DIMENSION_REF)
            {
                res.push_back(PARAM_IN_DIMENSION_NAME());
            }
        }

        return res;
    }

    void addDimension(Dimensions const& inputDims,
                      Dimensions& outDims,
                      shared_ptr<OperatorParam> const& param)
    {
        boost::shared_ptr<OperatorParamReference> const& reference =
            (boost::shared_ptr<OperatorParamReference> const&) param;

        string const& dimName = reference->getObjectName();
        string const& dimAlias = reference->getArrayName();

        for (size_t j = 0, n = inputDims.size(); j < n; j++)
        {
            if (inputDims[j].hasNameAndAlias(dimName, dimAlias))
            {
                //no overlap
                outDims.push_back(DimensionDesc( inputDims[j].getBaseName(),
                                                 inputDims[j].getNamesAndAliases(),
                                                 inputDims[j].getStartMin(),
                                                 inputDims[j].getCurrStart(),
                                                 inputDims[j].getCurrEnd(),
                                                 inputDims[j].getEndMax(),
                                                 inputDims[j].getChunkInterval(),
                                                 0));
                return;
            }
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST) << dimName;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        ArrayDesc const& input = schemas[0];

        Dimensions const& inputDims = input.getDimensions();

        Attributes outAttrs;
        Dimensions outDims;

        if (_parameters.empty())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2) << "aggregate";
        }

        removeDuplicateDimensions();

        for (size_t i = 0, n = _parameters.size(); i < n; ++i)
        {
            if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF)
            {
                    addDimension(inputDims, outDims, _parameters[i]);
            }
        }

        bool grand = true;
        if (outDims.size() == 0)
        {
            outDims.push_back(DimensionDesc("i", 0, 0, 0, 0, 1, 0));
        }
        else
        {
            _properties.tile = false;
            grand = false;
        }

        ArrayDesc outSchema(input.getName(), Attributes(), outDims);
        for (size_t i =0, n = _parameters.size(); i<n; i++)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                bool isInOrderAggregation = false;
                addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) _parameters[i], input,
                                        outSchema, isInOrderAggregation);
            }
        }

        if(!grand)
        {
            AttributeDesc et ((AttributeID) outSchema.getAttributes().size(),
                              DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                              TID_INDICATOR,
                              AttributeDesc::IS_EMPTY_INDICATOR,
                              0);
            outSchema.addAttribute(et);
        }
        return outSchema;
    }

private:

    /**
     * Remove duplicate dimension references from the parameter list.
     */
    void removeDuplicateDimensions()
    {
        typedef std::pair<std::string, std::string> NameAndAlias;
        typedef mgd::set<NameAndAlias> DimNameSet;

        DimNameSet seen;
        LogicalOperator::Parameters tmp;
        tmp.reserve(_parameters.size());

        for (size_t i = 0; i < _parameters.size(); ++i) {
            if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF) {
                const OperatorParamDimensionReference* dimRef =
                    safe_dynamic_cast<OperatorParamDimensionReference*>(_parameters[i].get());
                NameAndAlias key(dimRef->getObjectName(), dimRef->getArrayName());
                DimNameSet::iterator pos = seen.find(key);
                if (pos == seen.end()) {
                    tmp.push_back(_parameters[i]);
                    seen.insert(key);
                }
                // ...else a duplicate, ignore it.
            } else {
                tmp.push_back(_parameters[i]);
            }
        }

        _parameters.swap(tmp);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAggregate, "aggregate")

}  // namespace scidb
