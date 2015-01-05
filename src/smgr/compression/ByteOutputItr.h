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

#ifndef _BYTE_OUTPUT_ITR
#define _BYTE_OUTPUT_ITR

#include "stdint.h"
#include <vector>
#include <string.h>

namespace scidb {
  class ByteOutputItr
  {
  public:
    // initialize the array we are writing to
    ByteOutputItr(uint8_t * const dst, const size_t size);
    // put a single byte, returns 0 if successful, -1 if not successful
    int32_t put(const uint8_t e);

    // put an array of bytes of size size, returns 0 if successful, -1 if not successful
    int32_t putArray(const uint8_t * const a, const size_t size);

    // close out the iterator, returns bytes written
    size_t close()
    {
      return _written;
    }
    
    uint8_t * getWritePtr() { return _writePtr; } 

  private:
    uint8_t * _writePtr;
    size_t _written;
    size_t _max;
    
    

      
  };
}

#endif
