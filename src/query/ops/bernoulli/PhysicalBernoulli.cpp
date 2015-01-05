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
 *  Created on: Feb 15, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"
#include "system/SciDBConfigOptions.h"
#include "NumericOps.h"

using namespace std;
using namespace boost;

namespace scidb {

class BernoulliChunkIterator;

class BernoulliArrayIterator : public DelegateArrayIterator
{
    friend class BernoulliChunkIterator;
  public:
	virtual void operator ++()
    {
        while (nextElem < nChunkElems) { 
            nextElem += nops.geomdist(probability);
        }
        nextElem -= nChunkElems;
        ++(*inputIterator);
        while (!inputIterator->end()) { 
            nChunkElems = inputIterator->getChunk().count();
            if (nextElem < nChunkElems) {
                return;
            }
            nextElem -= nChunkElems;
            ++(*inputIterator);
        }
    }

    virtual bool setPosition(Coordinates const& pos) 
    {
        CoordinatesLess less;
        currPos = pos;
        inputDesc.getChunkPositionFor(currPos);
        if (end() || !less(inputIterator->getPosition(), currPos)) { 
            reset();
        }
        while (!end() && inputIterator->getPosition() != currPos) {
            ++(*this);
        }
        return !end();
    }

	virtual void reset() 
    { 
        inputIterator->reset();
        nops.ResetSeed(seed);
        nextElem = nops.geomdist(probability);
        while (!inputIterator->end()) { 
            nChunkElems = inputIterator->getChunk().count();
            if (nextElem < nChunkElems) {
                return;
            }
            nextElem -= nChunkElems;
            ++(*inputIterator);
        }
    }

    BernoulliArrayIterator(DelegateArray const& array, AttributeID attrID, shared_ptr<ConstArrayIterator> inputIterator,
                           double prob, int rndGenSeed)
    : DelegateArrayIterator(array, attrID, inputIterator),
      probability(prob), seed(rndGenSeed), threshold((int)(RAND_MAX*probability)),
      nops(rndGenSeed),
      inputDesc(array.getInputArray()->getArrayDesc()),
      isPlainArray(inputDesc.getEmptyBitmapAttribute() == NULL)
    {
        isNewEmptyIndicator = attrID >= array.getInputArray()->getArrayDesc().getAttributes().size();
        reset();
    }

  private:
    double probability;
    unsigned int seed;
    int threshold;
    NumericOperations nops;
    ArrayDesc const& inputDesc;
    size_t nextElem;
    size_t nChunkElems;
    bool isPlainArray;
    bool isNewEmptyIndicator;
    Coordinates currPos;
    
};


    class BernoulliChunkIterator : public DelegateChunkIterator
    {
      public:
        virtual void operator ++()
        {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
            nextElem += nops.geomdist(arrayIterator.probability);
            if (nextElem < arrayIterator.nChunkElems) { 
                setSamplePosition();
            } else { 
                hasCurrent = false;
            }
        }

        virtual bool end() 
        {
            return !hasCurrent;
        }
 
        virtual void reset() 
        { 
            hasCurrent = false;
        }

        virtual Value& getItem() { 
            return arrayIterator.isNewEmptyIndicator ? trueValue : DelegateChunkIterator::getItem();
        }

        BernoulliChunkIterator(DelegateChunk const* chunk, int iterationMode) 
        : DelegateChunkIterator(chunk, ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS),
          arrayIterator((BernoulliArrayIterator&)chunk->getArrayIterator()),                
          nops(arrayIterator.nops),
          nextElem(arrayIterator.nextElem),
          lastElem(0)
        {
            setSamplePosition();        
            trueValue.setBool(true);
        }

        void setSamplePosition() 
        {
            size_t offset = nextElem;
            if (!arrayIterator.isPlainArray) { 
                offset -= lastElem;
                while (offset-- != 0 && !inputIterator->end()) { 
                    ++(*inputIterator);
                }            
                lastElem = nextElem;
                hasCurrent = !inputIterator->end();
            } else {
                Coordinates pos = chunk->getFirstPosition(false);
                Coordinates const& last = chunk->getLastPosition(false);
            
                for (int i = (int)pos.size(); --i >= 0; ) {
                    size_t length = last[i] - pos[i] + 1;
                    pos[i] += offset % length;
                    offset /= length;
                }
                assert(offset == 0);
                hasCurrent = inputIterator->setPosition(pos);
            }
        }
        
      private:
        BernoulliArrayIterator& arrayIterator;
        NumericOperations nops;
        size_t nextElem;
        size_t lastElem;
        bool hasCurrent;
        Value trueValue;
    };

class BernoulliArray : public DelegateArray
{
  public:
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new BernoulliChunkIterator(chunk, iterationMode);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const 
    {
        return new BernoulliArrayIterator(*this, id, inputArray->getConstIterator(id < nAttrs ? id : 0), probability, seed);
    }

    BernoulliArray(ArrayDesc const& desc, boost::shared_ptr<Array> input, double prob, int rndGenSeed) 
    : DelegateArray(desc, input),
      probability(prob),
      seed(rndGenSeed)
    {
        nAttrs = input->getArrayDesc().getAttributes().size();
    }
    

  private:
    size_t nAttrs;
    double probability;
    int seed;
};

class PhysicalBernoulli: public PhysicalOperator
{
public:
	PhysicalBernoulli(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

	/***
	 * Bernoulli is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);

		shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

        int seed = (_parameters.size() == 2)
            ? (int)((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getInt64()
            : (int)time(NULL);
        double probability = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getDouble();
        if (seed < 0)
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_SAMPLE_ERROR1);
        if (probability <= 0 || probability > 1)
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_SAMPLE_ERROR2);
        return boost::shared_ptr<Array>(new BernoulliArray(_schema, inputArray, probability, seed));
    }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalBernoulli, "bernoulli", "physicalBernoulli")

}  // namespace scidb
