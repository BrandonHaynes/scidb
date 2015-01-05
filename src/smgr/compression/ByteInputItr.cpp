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

//#include "smgr/compression/ByteInputItr.h"
#include "ByteInputItr.h"


namespace scidb {

int32_t ByteInputItr::get(uint8_t &e)
    {
      if(_read + 1 <= _max)
	{
	  e = *_readPtr;
	  ++_readPtr;
	  ++_read;
	  return 0;
	}
     
      return -1;
    }

int32_t ByteInputItr::getArray(uint8_t * const a, const size_t size)
{

  if(_read + size <= _max)
    {
      memcpy(a, _readPtr, size);
      _readPtr += size;
      _read += size;
      return 0;
    }
  // too large, would create overwrite
  return -1;
}

}
