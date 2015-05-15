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

/**
 * @file
 *
 * @brief Run length encoding implementation
 *
 * @author Jennie Duggan (jennie@cs.brown.edu)
 */

#include <boost/scoped_array.hpp>
#include "smgr/compression/BuiltinCompressors.h"

//#include "log4cxx/logger.h"
//#include "log4cxx/basicconfigurator.h"
//#include "log4cxx/helpers/exception.h"


namespace scidb
{
    
    size_t RunLengthEncoding::compress(void* dst, const ConstChunk& chunk, size_t chunkSize) 
    {
        return chunkSize;
    }

  size_t RunLengthEncoding::decompress(void const* src, size_t size, Chunk& chunk)
  {

    size_t chunkSize = chunk.getSize();
    TypeId type = chunk.getAttributeDesc().getType();        
    size_t elementSize = TypeLibrary::getType(type).byteSize();
    size_t nElems;

    /* No more immutable arrays, to keep consistent with old code, always regard as string
     */
    nElems = chunkSize;
    elementSize = 1;

    char* dst = (char*)chunk.getDataForLoad();
    uint32_t i, j;

    size_t runs = size / (elementSize + 1); // 1 for run length delimiter
    char *srcPos = (char *) src;
    char *dstPos = dst;
    unsigned char runLength;
    uint32_t outputLength = 0;

    if(!nElems)
      {
	memcpy(dst, src, size);
	return chunkSize;
      }

    boost::scoped_array<char> runValueArr(new char[elementSize]);
    char *runValue = runValueArr.get(); 

    // iterate over the runs
    for(i = 0; i < runs; ++i)
      {
	memcpy(runValue, srcPos, elementSize);
	srcPos += elementSize;
	runLength = *srcPos;
	++srcPos;

	if(outputLength + runLength * elementSize <= chunkSize)
	  {
	    
	    for(j = 0; j < runLength; ++j)
	      {
		memcpy(dstPos, runValue, elementSize);
		dstPos += elementSize;
	      }
	    outputLength += runLength * elementSize;
	  }
	else
	  {
	    return chunkSize;
	  }

      }

    return outputLength;
  }
}
