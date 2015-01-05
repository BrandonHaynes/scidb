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

//#include "smgr/compression/BitInputItr.h"
#include "BitInputItr.h"

#include <iostream>
namespace scidb
{
  // get _bits contents, must request <= 8 bits
  int32_t BitInputItr::get(uint8_t &dst, const size_t bits)
  {
    uint8_t mask = 0;
    uint8_t i, start;
    
    uint8_t rhsLength, lhsLength;
    int32_t getValue;
    dst = 0;

    
    if(_bitsRead == 8)
      {
	_bitsRead = 0; 
	getValue = _src->get(_bits);
	if(getValue == -1) { return -1; } 
      }
    // all of it can be read from the current byte
    if(bits + _bitsRead <= 8) 
      {
	// create a bitmask for this
	for(i = 0; i < bits; ++i)
	  {
	    mask = mask | (1 << i);
	  }
	// now shift it all over to the start offset
	start = 8 - _bitsRead - bits;

	// get rid of rhs
	dst = _bits >> start;
	// get lower lhs bits
	dst = dst & mask;
	_bitsRead += bits;

	return 0;
      }
    else
      {
	lhsLength = 8 - _bitsRead;
	rhsLength = bits - lhsLength;
	
	for(i = 0; i < lhsLength; ++i)
	  {
	    mask = mask | (1 << i);
	  }

	dst = _bits & mask;
	dst = dst << rhsLength;

	getValue = _src->get(_bits);
	dst = dst | (_bits >> (8 - rhsLength)); 
	_bitsRead = rhsLength;
	
	return getValue;	
      }

  }
    
    
}
