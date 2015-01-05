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

//#include "smgr/compression/BitOutputItr.h"
#include "BitOutputItr.h"
#include <iostream>

namespace scidb
{
  // put bits lower-order bits, higher 8 minus bits must be 0 

int32_t BitOutputItr::put(const uint8_t e, const size_t bits)
  {
    uint8_t lhsLength, rhsLength;
    int32_t putValue;
    uint8_t shift;
    

    // we have to divide it among two bytes
    // modify this, copy over lhs length, then memcpy remaining bits, except rhs length, which is masked
    if(bits + _bitsWritten > 8) {
      lhsLength = 8 - _bitsWritten; // first byte
      rhsLength = bits - lhsLength; // second byte
      _bits = _bits | (e >> rhsLength);
      putValue = _dst->put(_bits);
      
      // set up for the next values
      _bits = 0;
      _bits = _bits | (e << (8-rhsLength));
      _bitsWritten = rhsLength;
      return putValue;
      
    }

    shift = 8 - _bitsWritten - bits;

    // else, just pack the bits
    _bits = _bits | (e << shift);
    _bitsWritten += bits;
    if(_bitsWritten == 8)
      {
	_bitsWritten = 0;
        putValue = _dst->put(_bits);
	_bits = 0;
	return putValue;
      }
	
    return 0;
  }
    
    
  int32_t BitOutputItr::flush()
  {
    if(_bitsWritten > 0)
      {
	
	return _dst->put(_bits);
      }
    else
      {
	return 0;
      }
  }


  int32_t BitOutputItr::close()
  {
    int32_t r = 0;
    if(_bitsWritten > 0)
      {
	r = _dst->put(_bits);
      }
    if(r == -1)
      { return -1; }
    return _dst->close();
  }
}
