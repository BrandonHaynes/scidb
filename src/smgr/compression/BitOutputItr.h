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

#ifndef _BIT_OUTPUT_ITR
#define _BIT_OUTPUT_ITR


//#include "smgr/compression/ByteOutputItr.h"
#include "ByteOutputItr.h"
#include "stdint.h"
#include <string.h>

namespace scidb
{
  class BitOutputItr
  {
  public:
  BitOutputItr(ByteOutputItr * const d) : _dst(d), _bits(0), _bitsWritten(0) {};
    
    // pack a set of right-aligned bits and pack them to the left, put completed byte to ByteOutputItr
    // bits must be 1...8
    int32_t put(const uint8_t e, const size_t bits);
    
    // push what we have to the _dst itr
    int32_t flush();

    // close out the _dst itr, return # of bytes written
    int32_t close(); 
    
  private:
    ByteOutputItr * const _dst;
    uint8_t _bits;
    uint8_t _bitsWritten;

  };

}


#endif
