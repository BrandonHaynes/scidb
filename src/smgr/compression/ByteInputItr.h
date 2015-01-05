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

#ifndef _BYTE_INPUT_ITR
#define _BYTE_INPUT_ITR

#include "stdint.h"
#include <string.h>

namespace scidb {
  class ByteInputItr
  {
  public:
    // initialize the array we are reading from
  ByteInputItr(uint8_t const * const src, const size_t size) : _readPtr(src), _read(0), _max(size) 
    {    };
    // put a single byte, returns 0 if successful, -1 if not successful
    int32_t get(uint8_t & e);

    // put an array of bytes of size size, returns 0 if successful, -1 if not successful
    int32_t getArray(uint8_t * const a, const size_t size);

    // close out the iterator, returns bytes written
    size_t close()
    {
      return _read;
    }

  private:
    uint8_t const *_readPtr;
    size_t _read;
    size_t _max;
    
  };
}

#endif
