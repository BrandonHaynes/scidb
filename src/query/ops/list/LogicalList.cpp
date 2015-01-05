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
 * @file LogicalList.cpp
 *
 * @author knizhnik@garret.ru
 *
 * List operator for listing data from external files into array
 */

#include "query/Parser.h"
#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"
#include "system/SystemCatalog.h"
#include "util/PluginManager.h"
#include <smgr/io/Storage.h>
#include "ListArrayBuilder.h"

namespace scidb
{

using namespace std;
using namespace boost;

/**
 * @brief The operator: list().
 *
 * @par Synopsis:
 *   list( what='arrays', showSystem=false )
 *
 * @par Summary:
 *   Produces a result array and loads data from a given file, and optionally stores to shadowArray.
 *   The available things to list include:
 *   - aggregates: show all the aggregate operators.
 *   - arrays: show all the arrays.
 *   - chunk descriptors: show all the chunk descriptors.
 *   - chunk map: show the chunk map.
 *   - functions: show all the functions.
 *   - instances: show all SciDB instances.
 *   - libraries: show all the libraries that are loaded in the current SciDB session.
 *   - operators: show all the operators and the libraries in which they reside.
 *   - types: show all the datatypes that SciDB supports.
 *   - queries: show all the active queries.
 *
 * @par Input:
 *   - what: what to list.
 *   - showSystem: whether to show systems information.
 *
 * @par Output array:
 *        <
 *   <br>   The list of attributes depends on the input.
 *   <br> >
 *   <br> [
 *   <br>   No: sequence number
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
class LogicalList: public LogicalOperator
{
public:
    LogicalList(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 0)
                res.push_back(PARAM_CONSTANT(TID_STRING));
        if (_parameters.size() == 1)
                res.push_back(PARAM_CONSTANT(TID_BOOL));
        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, boost::shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);

        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc(0,"name",TID_STRING,0,0);
        vector<DimensionDesc> dimensions(1);

        string what;
        bool showSystem = false;
        if (_parameters.size() >= 1)
        {
            what =  evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                             query,
                             TID_STRING).getString();
        }
        else
        {
            what = "arrays";
        }

        if (_parameters.size() == 2)
        {
            showSystem = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                query, TID_BOOL).getBool();
        }

        size_t size = 0;
        if (what == "aggregates") {
            size = AggregateLibrary::getInstance()->getNumAggregates();
        } else if (what == "arrays") {
            vector<string> arrays;
            SystemCatalog::getInstance()->getArrays(arrays);

            attributes.push_back(AttributeDesc(1,"id",            TID_INT64,0,0));
            attributes.push_back(AttributeDesc(2,"schema",        TID_STRING,0,0));
            attributes.push_back(AttributeDesc(3,"availability",  TID_BOOL,0,0));
            attributes.push_back(AttributeDesc(4,"temporary",     TID_BOOL,0,0));

            if (!showSystem)
            {
                vector<string>::iterator it = arrays.begin();

                //TODO: In future it better to have some flag in system catalog which will indicate that
                //      this object is system. For now we just check symbols which not allowed in user
                //      objects.
                while (it != arrays.end())
                {
                    if (it->find('@') != string::npos || it->find(':') != string::npos)
                    {
                        it = arrays.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
            }
            size = arrays.size();
        } else if (what == "operators") {
            vector<string> names;
            OperatorLibrary::getInstance()->getLogicalNames(names);
            size = names.size();
            attributes.push_back(AttributeDesc(1,"library",TID_STRING,0,0));
        } else if (what == "types") {
            size =  TypeLibrary::typesCount();
            attributes.push_back(AttributeDesc(1,"library",TID_STRING,0,0));
        } else if (what == "functions") {
            funcDescNamesMap& funcs = FunctionLibrary::getInstance()->getFunctions();
            for (funcDescNamesMap::const_iterator i = funcs.begin(); i != funcs.end(); ++i)
            {
                size += i->second.size();
            }
            size += 2; // for hardcoded iif and missing_reason
            attributes.push_back(AttributeDesc(1,"profile",      TID_STRING, 0,0));
            attributes.push_back(AttributeDesc(2,"deterministic",TID_BOOL,   0,0));
            attributes.push_back(AttributeDesc(3,"library",      TID_STRING, 0,0));
        } else if (what == "macros") {
            return logicalListMacros(); // see Parser.h
        } else if (what == "queries") {
            ListQueriesArrayBuilder builder;
            return builder.getSchema(query);
        } else if (what == "instances") {
            boost::shared_ptr<const InstanceLiveness> queryLiveness(query->getCoordinatorLiveness());
            size = queryLiveness->getNumInstances();
            attributes.reserve(5);
            attributes.push_back(AttributeDesc(1, "port",         TID_UINT16,0,0));
            attributes.push_back(AttributeDesc(2, "instance_id",  TID_UINT64,0,0));
            attributes.push_back(AttributeDesc(3, "online_since", TID_STRING,0,0));
            attributes.push_back(AttributeDesc(4, "instance_path",TID_STRING,0,0));
        } else if (what == "chunk descriptors" ) {
            ListChunkDescriptorsArrayBuilder builder;
            return builder.getSchema(query);
        } else if (what == "chunk map") {
            ListChunkMapArrayBuilder builder;
            return builder.getSchema(query);
        } else if (what == "libraries") {
            ListLibrariesArrayBuilder builder;
            return builder.getSchema(query);
        }
        else {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_LIST_ERROR1,
                        _parameters[0]->getParsingContext());
        }

        size_t chunkInterval = size>0 ? size : 1;
        dimensions[0] = DimensionDesc("No", 0, 0, chunkInterval-1, chunkInterval-1, chunkInterval, 0);
        return ArrayDesc(what, attributes, dimensions);
    }
};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalList, "list")


} //namespace
