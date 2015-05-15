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

#include <malloc.h>
#include <string.h>
#include <sstream>

#include <query/Parser.h>
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <array/TupleArray.h>
#include <array/DelegateArray.h>
#include <array/TransientCache.h>
#include <system/SystemCatalog.h>
#include <query/TypeSystem.h>
#include <util/PluginManager.h>
#include <smgr/io/Storage.h>
#include "ListArrayBuilder.h"

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace std;
using namespace boost;

struct PhysicalList : PhysicalOperator
{
    PhysicalList(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
     : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    string getMainParameter() const
    {
        if (_parameters.empty())
        {
            return "arrays";
        }

        return ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
    }

    bool coordinatorOnly() const
    {
        // The operations NOT in this list run exclusively on the coordinator

        static const char* const s[] =
        {
            "chunk descriptors",
            "chunk map",
            "datastores",
            "libraries",
            "meminfo",
            "queries",
        };

        return !std::binary_search(s,s+SCIDB_SIZE(s),getMainParameter().c_str(),less_strcmp());
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(coordinatorOnly() ? psLocalInstance : psUndefined);
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        if (coordinatorOnly() && !query->isCoordinator())
        {
            return make_shared<MemArray>(_schema,query);
        }

        vector<string> list;
        string  const  what = getMainParameter();

        if (what == "aggregates") {
            boost::shared_ptr<TupleArray> tuples(make_shared<TupleArray>(_schema,_arena));
            AggregateLibrary::getInstance()->getAggregateNames(list);
            for (size_t i=0, n=list.size(); i!=n; ++i) {
                 Value tuple;
                 tuple.setString(list[i]);
                 tuples->appendTuple(tuple);
             }
             return tuples;
        } else if (what == "arrays") {

            bool showAllArrays = false;
            if (_parameters.size() == 2)
            {
                showAllArrays = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getBool();
            }
            return listArrays(showAllArrays, query);

        } else if (what == "operators") {
            OperatorLibrary::getInstance()->getLogicalNames(list);
            boost::shared_ptr<TupleArray> tuples(boost::make_shared<TupleArray>(_schema, _arena));
            for (size_t i=0, n=list.size(); i!=n; ++i) {
                Value tuple[2];
                tuple[0].setString(list[i]);
                tuple[1].setString(OperatorLibrary::getInstance()->getOperatorLibraries().getObjectLibrary(list[i]));
                tuples->appendTuple(tuple);
            }
            return tuples;
        } else if (what == "types") {
            list = TypeLibrary::typeIds();
            boost::shared_ptr<TupleArray> tuples(boost::make_shared<TupleArray>(_schema, _arena));
            for (size_t i=0, n=list.size(); i!=n; ++i) {
                Value tuple[2];
                tuple[0].setString(list[i]);
                tuple[1].setString(TypeLibrary::getTypeLibraries().getObjectLibrary(list[i]));
                tuples->appendTuple(tuple);
            }
            return tuples;
        } else if (what == "functions") {
            boost::shared_ptr<TupleArray> tuples(boost::make_shared<TupleArray>(_schema, _arena));
            funcDescNamesMap& funcs = FunctionLibrary::getInstance()->getFunctions();
            for (funcDescNamesMap::const_iterator i = funcs.begin();
                 i != funcs.end(); ++i)
            {
                for (funcDescTypesMap::const_iterator j = i->second.begin();
                     j != i->second.end(); ++j)
                {
                    Value tuple[4];
                    FunctionDescription const& func = j->second;
                    tuple[0].setString(func.getName());
                    tuple[1].setString(func.getMangleName());
                    tuple[2].setBool(func.isDeterministic());
                    tuple[3].setString(FunctionLibrary::getInstance()->getFunctionLibraries().getObjectLibrary(func.getMangleName()));
                    tuples->appendTuple(tuple);
                }
            }
            Value tuple1[4];
            tuple1[0].setString("iif");
            tuple1[1].setString("<any> iif(bool, <any>, <any>)");
            tuple1[2].setBool(true);
            tuple1[3].setString("scidb");
            tuples->appendTuple(tuple1);
            Value tuple2[4];
            tuple2[0].setString("missing_reason");
            tuple2[1].setString("int32 missing_reason(<any>)");
            tuple2[2].setBool(true);
            tuple2[3].setString("scidb");
            tuples->appendTuple(tuple2);
            return tuples;
        } else if (what == "macros") {
            return physicalListMacros(_arena); // see Parser.h
        } else if (what == "queries") {
            ListQueriesArrayBuilder builder;
            builder.initialize(query);
            boost::function<void (const boost::shared_ptr<scidb::Query>&)> f
            = boost::bind(&ListQueriesArrayBuilder::listElement, &builder, _1);
            scidb::Query::listQueries(f);
            return builder.getArray();
        } else if (what == "instances") {
            return listInstances(query);
        } else if (what == "chunk descriptors") {
            ListChunkDescriptorsArrayBuilder builder;
            builder.initialize(query);
            StorageManager::getInstance().listChunkDescriptors(builder);
            return builder.getArray();
        } else if (what == "chunk map") {
            ListChunkMapArrayBuilder builder;
            builder.initialize(query);
            StorageManager::getInstance().listChunkMap(builder);
            return builder.getArray();
        } else if (what == "libraries") {
            ListLibrariesArrayBuilder builder;
            builder.initialize(query);
            PluginManager::getInstance()->listPlugins(builder);
            return builder.getArray();
        } else if (what == "datastores") {
            ListDataStoresArrayBuilder builder;
            builder.initialize(query);
            StorageManager::getInstance().getDataStores().listDataStores(builder);
            return builder.getArray();
        } else if (what == "meminfo") {
            ListMeminfoArrayBuilder builder;
            builder.initialize(query);
            builder.addToArray(::mallinfo());
            return builder.getArray();
        } else if (what == "counters") {
            bool reset = false;
            if (_parameters.size() == 2)
            {
                reset = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)
                         _parameters[1])->getExpression()->evaluate().getBool();
            }
            ListCounterArrayBuilder builder;
            builder.initialize(query);
            CounterState::getInstance()->listCounters(builder);
            if (reset)
            {
                CounterState::getInstance()->reset();
            }
            return builder.getArray();
        }
        else
        {
            SCIDB_UNREACHABLE();
        }

        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "PhysicalList::execute";
     }

   boost::shared_ptr<Array> listInstances(const boost::shared_ptr<Query>& query)
   {
      boost::shared_ptr<const InstanceLiveness> queryLiveness(query->getCoordinatorLiveness());
      Instances instances;
      SystemCatalog::getInstance()->getInstances(instances);

      assert(queryLiveness->getNumInstances() == instances.size());

      boost::shared_ptr<TupleArray> tuples(boost::make_shared<TupleArray>(_schema, _arena));

      for (Instances::const_iterator iter = instances.begin();
           iter != instances.end(); ++iter) {

          Value tuple[5];

          const InstanceDesc& instanceDesc = *iter;
          InstanceID instanceId = instanceDesc.getInstanceId();
          time_t t = static_cast<time_t>(instanceDesc.getOnlineSince());
          tuple[0].setString(instanceDesc.getHost());
          tuple[1].setUint16(instanceDesc.getPort());
          tuple[2].setUint64(instanceId);
          if ((t == (time_t)0) || queryLiveness->isDead(instanceId)){
              tuple[3].setString("offline");
          } else {
              assert(queryLiveness->find(instanceId));
              struct tm date;

              if (!(&date == gmtime_r(&t, &date)))
                  throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_GENERATE_UTC_TIME);

              string out(boost::str(boost::format("%04d-%02d-%02d %02d:%02d:%02d")
                                    % (date.tm_year+1900)
                                    % (date.tm_mon+1)
                                    % date.tm_mday
                                    % date.tm_hour
                                    % date.tm_min
                                    % date.tm_sec));
              tuple[3].setString(out);
          }
          tuple[4].setString(instanceDesc.getPath());
          tuples->appendTuple(tuple);
      }
      return tuples;
   }

    boost::shared_ptr<Array> listArrays(bool showAllArrays, const boost::shared_ptr<Query>& query)
    {
        ListArraysArrayBuilder builder;
        builder.initialize(query);

        vector<ArrayDesc> arrayDescs;
        const bool ignoreOrphanAttributes = true;

        SystemCatalog::getInstance()->getArrays(arrayDescs,
                                                ignoreOrphanAttributes,
                                                !showAllArrays);

        for (size_t i=0, n=arrayDescs.size(); i!=n; ++i) {
            const ArrayDesc& desc = arrayDescs[i];
            builder.listElement(desc);
        }
        return builder.getArray();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalList, "list", "physicalList")

/****************************************************************************/
}
/****************************************************************************/
