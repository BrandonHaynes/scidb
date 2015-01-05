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
//#include "network/NetworkManager.h"

#include "Cook.h"
#include "PixelProvider.h"

namespace scidb	{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("Findstars"));

class	PhysicalFindStars: public PhysicalOperator
{
public:

PhysicalFindStars(const std::string& logicalName, const std::string& physicalName,
                  const Parameters& parameters, const ArrayDesc& schema):
                  PhysicalOperator(logicalName, physicalName, parameters, schema)
{}

boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,boost::shared_ptr< Query> query)
{
  boost::shared_ptr<Array> inputArray = inputArrays[0];
  const ArrayDesc &arrayDesc = inputArray->getArrayDesc();
  DimensionDesc xDim= arrayDesc.getDimensions()[2];
  DimensionDesc yDim= arrayDesc.getDimensions()[1];
  DimensionDesc zDim= arrayDesc.getDimensions()[0];
  AttributeID aid = 0; // default attribute pix
  TypeId attType = TID_INT32;
  int32_t threshold = 1000; // default threashold
  Value value = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate();
  if (!value.isNull())
  {
    threshold = value.getInt32();
  }
  Attributes attributes = arrayDesc.getAttributes();

  aid = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectNo();
  attType = attributes[aid].getType();

  boost::shared_ptr<ConstArrayIterator> aItr = inputArray->getConstIterator(aid);
    
  boost::shared_ptr<MemArray> outputArray = boost::shared_ptr<MemArray>(new MemArray(_schema,query));
  ImageProvider provider(aItr, outputArray, aid);
  Cook cook(provider, threshold);
  while(!aItr->end())
  {
    cook.cookRawImage();
    LOG4CXX_DEBUG(logger, "Cooking image: " << aItr->getPosition()[0]);
    ++(*aItr);
  }
  provider.onFinalize();
  return outputArray;
}
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFindStars, "findstars", "physicalFindStars");
}
