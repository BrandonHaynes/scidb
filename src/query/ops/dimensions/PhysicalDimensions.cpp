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
 * @file PhysicalDimensions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of DIMENSIONS operator for dimensioning data from text files
 */

#include <string.h>

#include <query/Operator.h>
#include <array/TupleArray.h>
#include <system/SystemCatalog.h>

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalDimensions: public PhysicalOperator
{
public:
    PhysicalDimensions(std::string const& logicalName,
                       std::string const& physicalName,
                       Parameters const& parameters,
                       ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const& inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        return ArrayDistribution(psLocalInstance);
    }

    void preSingleExecute(boost::shared_ptr<Query> query)
    {
        assert(_parameters.size() == 1);
        string arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        SystemCatalog& catalog = * SystemCatalog::getInstance();
        ArrayDesc arrayDesc;
        catalog.getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
        Coordinates lowBoundary = catalog.getLowBoundary(arrayDesc.getId());
        Coordinates highBoundary = catalog.getHighBoundary(arrayDesc.getId());
        Dimensions const& dims = arrayDesc.getDimensions();
        
        boost::shared_ptr<TupleArray> tuples(boost::make_shared<TupleArray>(_schema, _arena));
        for (size_t i = 0, size = dims.size(); i < size; i++)
        {
            Value tuple[8];
            tuple[0].setData(dims[i].getBaseName().c_str(), dims[i].getBaseName().length() + 1);
            tuple[1] = Value(TypeLibrary::getType(TID_INT64));
            tuple[1].setInt64(dims[i].getStartMin());
            tuple[2] = Value(TypeLibrary::getType(TID_UINT64));
            tuple[2].setUint64(dims[i].getLength());
            tuple[3] = Value(TypeLibrary::getType(TID_UINT64));
            tuple[3].setUint64(dims[i].getChunkInterval());
            tuple[4] = Value(TypeLibrary::getType(TID_UINT64));
            tuple[4].setUint64(dims[i].getChunkOverlap());
            tuple[5] = Value(TypeLibrary::getType(TID_INT64));
            tuple[5].setInt64(lowBoundary[i]);
            tuple[6] = Value(TypeLibrary::getType(TID_INT64));
            tuple[6].setInt64(highBoundary[i]);
            tuple[7].setString(TID_INT64); //TODO-3667: remove type from dimensions output. NOTE: requires a lot of test changes

            tuples->appendTuple(tuple);
        }

        _result = tuples;
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        if (!_result)
        {
            _result = boost::make_shared<MemArray>(_schema, query);
        }
        return _result;
    }

private:
    boost::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalDimensions, "dimensions", "physicalDimensions")

} //namespace
