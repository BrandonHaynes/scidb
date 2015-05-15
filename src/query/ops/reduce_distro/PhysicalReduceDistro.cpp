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
 * PhysicalProject.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"
#include "system/Cluster.h"

namespace scidb
{

using namespace boost;
using namespace std;



class ReduceDistroArrayIterator : public DelegateArrayIterator
{
public:
   ReduceDistroArrayIterator(const boost::shared_ptr<Query>& query,
                             DelegateArray const& delegate,
                             AttributeID attrID,
                             shared_ptr<ConstArrayIterator> inputIterator,
                             PartitioningSchema ps):
   DelegateArrayIterator(delegate, attrID, inputIterator), _ps(ps), _myInstance(query->getInstanceID()), _nextChunk(0), _query(query)
    {
        findNext();
    }


    virtual ~ReduceDistroArrayIterator()
    {}

    virtual void findNext()
    {
        while (!inputIterator->end())
        {
            Coordinates const& pos = inputIterator->getPosition();
            shared_ptr<DistributionMapper> distMapper;
            if (getInstanceForChunk(Query::getValidQueryPtr(_query), pos,
                                    array.getArrayDesc(), _ps,
                                    distMapper, 0, 0) == _myInstance)
            {
                _nextChunk = &inputIterator->getChunk();
                _hasNext = true;
                return;
            }

            ++(*inputIterator);
        }
        _hasNext = false;
    }

    virtual void reset()
    {
        chunkInitialized = false;
        inputIterator->reset();
        findNext();
    }

    bool end()
    {
        return _hasNext == false;
    }

    void operator ++()
    {
        chunkInitialized = false;
        ++(*inputIterator);
        findNext();
    }

    bool setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        shared_ptr<DistributionMapper> distMapper;
        if (getInstanceForChunk(Query::getValidQueryPtr(_query), pos, array.getArrayDesc(),
                                _ps, distMapper, 0, 0) == _myInstance &&
            inputIterator->setPosition(pos))
        {
            _nextChunk = &inputIterator->getChunk();
            return _hasNext = true;
        }

        return _hasNext = false;
    }

    ConstChunk const& getChunk()
    {
        if (!chunkInitialized)  {
            chunk->setInputChunk(*_nextChunk);
            chunkInitialized = true;
        }
        return *chunk;
    }


private:
    bool _hasNext;
    PartitioningSchema _ps;
    InstanceID _myInstance;
    ConstChunk const* _nextChunk;
    boost::weak_ptr<Query> _query;
};



class ReduceDistroArray: public DelegateArray
{
public:
    ReduceDistroArray(const boost::shared_ptr<Query>& query, ArrayDesc const& desc, boost::shared_ptr<Array> const& array, PartitioningSchema ps):
    DelegateArray(desc, array, true), _ps(ps)
    {
        assert(query);
        _query = query;
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const
    {
        return new ReduceDistroArrayIterator(Query::getValidQueryPtr(_query),
                                             *this, id, inputArray->getConstIterator(id), _ps);
    }

private:
   PartitioningSchema _ps;
};

class PhysicalReduceDistro: public  PhysicalOperator
{
public:
    PhysicalReduceDistro(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
        :  PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    virtual bool changesDistribution(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector<ArrayDesc> const&) const
    {
        PartitioningSchema ps = (PartitioningSchema)((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();
        return ArrayDistribution(ps);
    }

	/***
	 * Project is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
        PartitioningSchema ps = (PartitioningSchema)((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();
        return boost::shared_ptr<Array>(new ReduceDistroArray(query, _schema, inputArrays[0], ps));
	 }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalReduceDistro, "reduce_distro", "physicalReduceDistro")

}  // namespace scidb
