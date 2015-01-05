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
 * 
 *
 *  Created on: 
 *      Author: 
 */

#include "log4cxx/logger.h"
#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "grouper.h"
#include "cookgroup.h"

namespace scidb	{
#define ITER_MODE ConstChunkIterator::IGNORE_EMPTY_CELLS | ConstChunkIterator::IGNORE_NULL_VALUES

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("Groupstars"));

class	PhysicalGroupStars: public PhysicalOperator
{
public:

PhysicalGroupStars(const std::string& logicalName, const std::string& physicalName,
                  const Parameters& parameters, const ArrayDesc& schema):
                  PhysicalOperator(logicalName, physicalName, parameters, schema)
{}

boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,boost::shared_ptr< Query> query)
{
  boost::shared_ptr<Array> obsArray = inputArrays[0];
  boost::shared_ptr<Array> spaceArray = inputArrays[1];
   
  if ( query->getInstancesCount() > 1) {
    uint64_t coordinatorID = (int64_t)query->getCoordinatorID() == -1 ? query->getInstanceID() : query->getCoordinatorID();
    obsArray = redistribute(obsArray, query, psLocalInstance, "", coordinatorID);
    spaceArray = redistribute(spaceArray, query, psLocalInstance, "", coordinatorID);
    if ( query->getInstanceID() != coordinatorID) {
        return boost::shared_ptr<MemArray>(new MemArray(_schema,query));
    }
  }

  std::vector<ObsPos> allObs;
  std::vector<Image> allImages;

  float D2 = 0.2; // default velocity
  int32_t T= 20; // default backtracking
  Value value_d = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate();
  Value value_t = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate();
  if(!value_d.isNull()) D2= value_d.getDouble();
  if(!value_t.isNull()) T= value_t.getInt32();

  AttributeID I=0;
  AttributeID J=1;
  AttributeID index=2;
  boost::shared_ptr<ConstArrayIterator> indexItr = spaceArray->getConstIterator(index);
  boost::shared_ptr<ConstChunkIterator> indexCtr;
  boost::shared_ptr<ConstArrayIterator> IItr = spaceArray->getConstIterator(I);
  boost::shared_ptr<ConstChunkIterator> ICtr;
  boost::shared_ptr<ConstArrayIterator> JItr = spaceArray->getConstIterator(J);
  boost::shared_ptr<ConstChunkIterator> JCtr;


  while(!indexItr->end())
  {
    indexCtr= indexItr->getChunk().getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
    ICtr= IItr->getChunk().getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
    JCtr= JItr->getChunk().getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
    while(!indexCtr->end())
    {
      Value idx=indexCtr->getItem();
      Value i=ICtr->getItem();
      Value j=JCtr->getItem();
      allImages.push_back(Image(idx.getInt32(),i.getInt32(),j.getInt32(),i.getInt32()+7499,j.getInt32()+7499,idx.getInt32(),idx.getInt32()/20));
      ++(*indexCtr);
      ++(*ICtr);
      ++(*JCtr);
    }
    ++(*indexItr);
    ++(*IItr);
    ++(*JItr);
  }

  LOG4CXX_DEBUG(logger, "Fetched all images: " << allImages.size());

  AttributeID oid=0;
  boost::shared_ptr<ConstArrayIterator> aItr = obsArray->getConstIterator(oid);
  boost::shared_ptr<ConstChunkIterator> cItr;
  int i=0; 
  while(!aItr->end())
  {
    cItr= aItr->getChunk().getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);  
    while(!cItr->end())
    {
      Value obs=cItr->getItem();
      Coordinates pos=cItr->getPosition(); 
      allObs.push_back(ObsPos(obs.getInt32(), pos[0], pos[1]+allImages[i].xstart, pos[2]+allImages[i].ystart));
      ++(*cItr);
    }
    ++(*aItr);
  }
  LOG4CXX_DEBUG(logger, "Fetched all observations: " << allObs.size());
  Grouper grouper;
  grouper.loadGroup(allObs,allImages,D2,T);
  LOG4CXX_DEBUG(logger, "Storing the groups: " << grouper.getSize());
  boost::shared_ptr<MemArray> outputArray = boost::shared_ptr<MemArray>(new MemArray(_schema,query));
  grouper.storeGroup(outputArray);
  LOG4CXX_DEBUG(logger, "Done, now return.");
  return outputArray;
}
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupStars, "groupstars", "physicalGroupStars");
}
