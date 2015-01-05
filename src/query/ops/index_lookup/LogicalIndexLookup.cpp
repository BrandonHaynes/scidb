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

#include <query/Operator.h>
#include "IndexLookupSettings.h"

namespace scidb
{

/**
 * An example operator that uses an attribute from input array A to look up the coordinate value from another input
 * array B. The second argument B must be sorted, dense, one-dimensional and single-attribute. For example, suppose
 * A is a large array of stock trades, where one of the attributes is the stock symbol string. Suppose B is a sorted
 * list of all the unique stock symbols on the exchange. The operator index_lookup can then be used to convert each
 * stock symbol string in A to its integer coordinate in B. The looked-up coordinate might then be used to redimension
 * A into another shape.
 * <br>
 * <br>
 * The operator introduces the virtual array pattern, provides an example call to redistribute(), shows an example of
 * random-access array reading and illustrates some strategies for dealing with data that is too large to fit in memory.
 *
 * @brief The operator: index_lookup()
 *
 * @par Synopsis: index_lookup (input_array, index_array, input_array.attribute_name [,output_attribute_name] [,'memory_limit=MEMORY_LIMIT'])
 *
 * @par Examples:
 *   <br> index_lookup(stock_trades, stock_symbols, stock_trades.ticker)
 *   <br> index_lookup(stock_trades, stock_symbols, stock_trades.ticker, ticker_id, 'memory_limit=1024')
 *
 * @par Summary:
 *   <br>
 *   The input_array may have any attributes or dimensions. The index_array must have a single dimension and a single
 *   non-nullable attribute. The index array data must be sorted, unique values with no empty cells between them (though
 *   it does not necessarily need to be populated to the upper bound). The third argument must correctly refer to one
 *   of the attributes of the input array - the looked-up attribute. This attribute must have the same datatype as the
 *   only attribute of the index array. The comparison "<" function must be registered in SciDB for this datatype.
 *   <br>
 *   <br>
 *   The operator will create a new attribute, named input_attribute_name_index by default, or using the provided name,
 *   which will be the new last non-empty-tag attribute in the output array. The output attribute will be of type int64
 *   nullable and will contain the respective coordinate of the corresponding input_attribute in index_array. If the
 *   corresponding input_attribute is null, or if no value for input_attribute exists in the index_array, the output
 *   attribute at that position shall be set to null. The output attribute shall be returned along all the input
 *   attributes in a fashion similar to the apply() operator.
 *   <br>
 *   <br>
 *   The operator uses some memory to cache a part of the index_array for fast lookup of values. By default, the size
 *   of this cache is limited to MEM_ARRAY_THRESHOLD. Note this is in addition to the memory already consumed by cached
 *   MemArrays as the operator is running. If a larger or smaller limit is desired, the 'memory_limit' parameter may be
 *   used. It is provided in units of mebibytes and must be at least 1.
 *   <br>
 *   <br>
 *   The operator may be further optimized to reduce memory footprint, optimized with a more clever data distribution
 *   pattern and/or extended to use multiple index arrays at the same time.
 *
 * @par Input:
 *   <br> input_array <..., input_attribute: type,... > [*]
 *   <br> index_array <index_attribute: type not null> [dimension=0:any,any,any]
 *   <br> input_attribute                --the name of the input attribute
 *   <br> [output_attribute_name]        --the name for the output attribute if desired
 *   <br> ['memory_limit=MEMORY_LIMIT']  --the memory limit to use MB)
 *
 * @par Output array:
 *   <br> <
 *   <br>   ...
 *   <br>   input_attribute_name:type
 *   <br>   ...
 *   <br>   output_attribute:int64 null  --default name is input_attribute_name+"_index", i.e. "stock_symbol_index".
 *   <br> >
 *   <br> [ * ]
 *   <br>
 *
 * @see PhysicalIndexLookup.cpp for a description of the algorithm.
 * The code assumes familiarity with the concepts described in hello_instances nad instance_stats. Consider reading
 * those operators first.
 * @see LogicalHelloInstances.cpp
 * @see LogicalInstanceStats.cpp
 * @author apoliakov@paradigm4.com
 */
class LogicalIndexLookup : public LogicalOperator
{
public:
    LogicalIndexLookup(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
        ADD_PARAM_IN_ATTRIBUTE_NAME("void") //the input attribute name is compulsory
        ADD_PARAM_VARIES()
    }

    vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(vector< ArrayDesc> const& schemas)
    {
        //Same settings pattern as seen in instance_stats, uniq
        vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < IndexLookupSettings::MAX_PARAMETERS)
        {
            //However, for the output attribute name we use SciDB parameter parsing instead of a string
            //This allows us to leverage SciDB's built-in check for a valid identifier
            res.push_back(PARAM_OUT_ATTRIBUTE_NAME("void"));

            //The string parameter is used for the memory_limit setting
            res.push_back(PARAM_CONSTANT(TID_STRING));
        }
        return res;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& input = schemas[0];
        ArrayDesc const& index = schemas[1];
        //The settings object also checks the input schemas for validity
        IndexLookupSettings settings(input, index, _parameters, true, query);
        ArrayDesc result (input.getName(), input.getAttributes(true), input.getDimensions());
        AttributeDesc newAttribute(input.getAttributes(true).size(),
                                   settings.getOutputAttributeName(),
                                   TID_INT64,
                                   AttributeDesc::IS_NULLABLE,
                                   0);
        result.addAttribute(newAttribute);     //good method: it checks to make sure the name of newAttribute is unique
        result = addEmptyTagAttribute(result);
        return result;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalIndexLookup, "index_lookup")

} //namespace scidb
