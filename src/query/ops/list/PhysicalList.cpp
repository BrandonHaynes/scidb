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
 * @file PhysicalList.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of LIST operator for listing data from text files
 */

#include <string.h>
#include <sstream>

#include "query/Parser.h"
#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/TupleArray.h"
#include "array/TransientCache.h"
#include "system/SystemCatalog.h"
#include "query/UDT.h"
#include "query/TypeSystem.h"
#include "util/PluginManager.h"
#include <smgr/io/Storage.h>
#include "ListArrayBuilder.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalList: public PhysicalOperator
{
public:
    PhysicalList(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    string getMainParameter() const
    {
        string what;
        if (_parameters.size() >= 1)
        {
            what = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
        }
        else
        {
            what = "arrays";
        }
        return what;
    }

    bool coordinatorOnly() const
    {
        if(getMainParameter() == "chunk descriptors" || getMainParameter() == "chunk map" ||
           getMainParameter() == "libraries" || getMainParameter() == "queries")
        {
            return false;
        }
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        if ( !coordinatorOnly() )
        {
            return ArrayDistribution(psUndefined);
        }
        return ArrayDistribution(psLocalInstance);
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        vector<string> list;
        string what = getMainParameter();
        bool showSystem = false;

        if( coordinatorOnly() && query->getCoordinatorID() != COORDINATOR_INSTANCE )
        {
            return shared_ptr<Array> (new MemArray(_schema,query));
        }

        if (_parameters.size() == 2)
        {
            showSystem = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getBool();
        }

        if (what == "aggregates") {
            AggregateLibrary::getInstance()->getAggregateNames(list);
            vector< boost::shared_ptr<Tuple> > tuples(list.size());
            for (size_t i = 0; i < tuples.size(); i++) {
                Tuple& tuple = *new Tuple(1);
                tuples[i] = boost::shared_ptr<Tuple>(&tuple);
                tuple[0].setString(list[i].c_str());
            }
            return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
        } else if (what == "arrays") {
            SystemCatalog::getInstance()->getArrays(list);
            if (!showSystem)
            {
                vector<string>::iterator it = list.begin();
                while(it != list.end())
                {
                    if (it->find('@') != string::npos || it->find(':') != string::npos)
                    {
                        it = list.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
            }
            vector< boost::shared_ptr<Tuple> > tuples(list.size());
            ArrayDesc desc;
            stringstream ss;
            for (size_t i = 0; i < tuples.size(); i++) {
                Tuple& tuple = *new Tuple(5);
                tuples[i] = boost::shared_ptr<Tuple>(&tuple);
                tuple[0].setString(list[i].c_str());

                bool available = true;
                boost::shared_ptr<Exception> exception;
                SystemCatalog::getInstance()->getArrayDesc(list[i], desc, false, exception);

                if (desc.isInvalid())
                {
                    available = false;
                }
                else
                if (exception.get())
                {
                    if (exception->getLongErrorCode() == SCIDB_LE_TYPE_NOT_REGISTERED ||
                        exception->getLongErrorCode() == SCIDB_LE_FUNCTION_NOT_FOUND)
                    {
                        available = false;
                    }
                    else
                    {
                        exception->raise();
                    }
                }
                tuple[1].setInt64(desc.getId());

                ss.str("");
                printSchema(ss, desc);
                tuple[2].setString(ss.str().c_str());
                tuple[3].setBool(available);
                tuple[4].setBool(desc.isTransient());
            }
            return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
        } else if (what == "operators") {
            OperatorLibrary::getInstance()->getLogicalNames(list);
            vector< boost::shared_ptr<Tuple> > tuples(list.size());
            for (size_t i = 0; i < tuples.size(); i++) {
                Tuple& tuple = *new Tuple(2);
                tuples[i] = boost::shared_ptr<Tuple>(&tuple);
                tuple[0].setData(list[i].c_str(), list[i].length() + 1);
                const string& libraryName = OperatorLibrary::getInstance()->getOperatorLibraries().getObjectLibrary(list[i]);
                tuple[1].setString(libraryName.c_str());
            }
            return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
        } else if (what == "types") {
#if 0
            printf("Matrix<double>(10,10):\n");
            Matrix<double> m(10, 10);
            someGenericAlgorithm<Matrix<double>, double>(m);
            printf("MatrixOfUDT(10,10):\n");
            MatrixOfUDT mUDT(TID_DOUBLE, 10, 10);
            someGenericAlgorithm<MatrixOfUDT, UDT::Val>(mUDT);
#endif
            list = TypeLibrary::typeIds();
            vector< boost::shared_ptr<Tuple> > tuples(list.size());
            for (size_t i = 0; i < tuples.size(); i++) {
                Tuple& tuple = *new Tuple(2);
                tuples[i] = boost::shared_ptr<Tuple>(&tuple);
                tuple[0].setData(list[i].c_str(), list[i].length() + 1);
                const string& libraryName = TypeLibrary::getTypeLibraries().getObjectLibrary(list[i]);
                tuple[1].setString(libraryName.c_str());
            }
            return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
        } else if (what == "functions") {
            vector<boost::shared_ptr<Tuple> > tuples;
            funcDescNamesMap& funcs = FunctionLibrary::getInstance()->getFunctions();
            for (funcDescNamesMap::const_iterator i = funcs.begin();
                 i != funcs.end(); ++i)
            {
                for (funcDescTypesMap::const_iterator j = i->second.begin();
                     j != i->second.end(); ++j)
                {
                    Tuple& tuple = *new Tuple(4);
                    FunctionDescription const& func = j->second;
                    tuples.push_back(shared_ptr<Tuple>(&tuple));
                    tuple[0].setString(func.getName().c_str());
                    const string mangleName = func.getMangleName();
                    tuple[1].setString(mangleName.c_str());
                    tuple[2].setBool(func.isDeterministic());
                    const string& libraryName = FunctionLibrary::getInstance()->getFunctionLibraries().getObjectLibrary(mangleName);
                    tuple[3].setString(libraryName.c_str());
                }
            }
            Tuple& tuple1 = *new Tuple(4);
            tuples.push_back(shared_ptr<Tuple>(&tuple1));
            tuple1[0].setString("iif");
            tuple1[1].setString("<any> iif(bool, <any>, <any>)");
            tuple1[2].setBool(true);
            tuple1[3].setString("scidb");
            Tuple& tuple2 = *new Tuple(4);
            tuples.push_back(shared_ptr<Tuple>(&tuple2));
            tuple2[0].setString("missing_reason");
            tuple2[1].setString("int32 missing_reason(<any>)");
            tuple2[2].setBool(true);
            tuple2[3].setString("scidb");
            return shared_ptr<Array>(new TupleArray(_schema, tuples));
        } else if (what == "macros") {
            return physicalListMacros(); // see Parser.h
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
         }
         else {
           assert(0);
         }

        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "PhysicalList::execute";
        return boost::shared_ptr<Array>();
    }

   boost::shared_ptr<Array> listInstances(const boost::shared_ptr<Query>& query)
   {
      boost::shared_ptr<const InstanceLiveness> queryLiveness(query->getCoordinatorLiveness());
      Instances instances;
      SystemCatalog::getInstance()->getInstances(instances);

      assert(queryLiveness->getNumInstances() == instances.size());

      vector<boost::shared_ptr<Tuple> > tuples;
      tuples.reserve(instances.size());
      for (Instances::const_iterator iter = instances.begin();
           iter != instances.end(); ++iter) {

         shared_ptr<Tuple> tuplePtr(new Tuple(5));
         Tuple& tuple = *tuplePtr.get();
         tuples.push_back(tuplePtr);

         const InstanceDesc& instanceDesc = *iter;
         InstanceID instanceId = instanceDesc.getInstanceId();
         time_t t = static_cast<time_t>(instanceDesc.getOnlineSince());
         tuple[0].setString(instanceDesc.getHost().c_str());
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
            tuple[3].setString(out.c_str());
         }
         tuple[4].setString(instanceDesc.getPath().c_str());
      }
      return shared_ptr<Array>(new TupleArray(_schema, tuples));
   }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalList, "list", "physicalList")

} //namespace
