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

#ifndef _BIT_INPUT_ITR
#define _BIT_INPUT_ITR

//#include "smgr/compression/ByteInputItr.h"
#include "ByteInputItr.h"
#include "stdint.h"
#include <string.h>

namespace scidb
{
  class BitInputItr
  {
  public:
  BitInputItr(ByteInputItr * const s) : _src(s), _bitsRead(0) {
      if(_src->get(_bits) == -1)
	{
	  // set bits read to 8 to trigger a fail on the next read
	  _bitsRead = 8;
	}
    };
    
    // pack a set of right-aligned bits and pack them to the left, put completed byte to ByteInputItr
    // bits must be 1...8
    int32_t get(uint8_t &dst, const size_t bits);
    
     // close out the _src itr, return # of bytes written
    int32_t close()
    {
      return _src->close();
    }
    
  private:
    ByteInputItr * const _src;
    uint8_t _bits; // current byte from input that we are working from
    uint8_t _bitsRead; // # of bits we have processed from _bits

  };
}



#endif
