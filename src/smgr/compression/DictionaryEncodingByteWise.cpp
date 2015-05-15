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
 * @brief Dictionary encoding implementation
 *
 * @author Jennie Duggan (jennie@cs.brown.edu)
 */

#include <cmath>
#include <boost/scoped_array.hpp>
#include <iostream>
#include <stdio.h>
#include <fstream>

#include "smgr/compression/BuiltinCompressors.h"
#include "system/Sysinfo.h"

// create a dictionary that has a fixed mapping of codes to values
// dictionary of uint32s to values

// two passes: one to build dictionary, second to output dictionary + values
// codes of fixed length, but not necessarily byte-aligned

namespace scidb
{


  // returns dictionary size in bytes
  size_t DictionaryEncoding::dictionarySize(uint32_t uniqueElements, size_t elementSize, uint8_t bytesPerCode)
  {
    size_t bitsPerEntry = ceil(log2(uniqueElements)); // size of an individual dictionary encoding
    bitsPerEntry = (bitsPerEntry==0) ? 1 : bitsPerEntry; 
    size_t bitsPerCode = 8 * bytesPerCode;
    uint32_t valuesPerCode = floor(bitsPerCode / bitsPerEntry);
    uint32_t dictionaryEntries = pow(uniqueElements,valuesPerCode);
    size_t entrySize = bytesPerCode + elementSize * valuesPerCode;
    return dictionaryEntries * entrySize;

  }

  // builds for encode and decode
  void DictionaryEncoding::generateAllCodes(const uint32_t level, const uint32_t code, const std::string value, const uint32_t entryBits, const size_t valueBytes)
  {
    uint32_t i;
    uint32_t myCode;
    std::string myValue;
    uint32_t uniqueValues = _values.size();

    if(level == 0)
      {
	_encodeDictionary[value] = code;
	_decodeDictionary[code] = value;
	return;
      }

    for(i = 0; i < uniqueValues; ++i)
      {
	// append my code
	myCode = (code << entryBits) | i;
	myValue = value + _values[i];
	generateAllCodes(level-1,  myCode, myValue, entryBits, valueBytes);
      }
  }


  // to be called after _values is populated with a sorted list of all unique element values
  int32_t DictionaryEncoding::generateDictionary(const size_t elementSize)
  {
    // cache size in bytes, take no greater than 3/4 of it to prevent thrashing
    size_t maxCache = Sysinfo::getCPUCacheSize(Sysinfo::CPU_CACHE_L2 | Sysinfo::CPU_CACHE_L3) * 3 / 4;
    uint32_t entryLengthBits = ceil(log2(_values.size()));
    entryLengthBits = (entryLengthBits==0) ? 1 : entryLengthBits;  

    uint32_t entriesPerCode = floor(8 / entryLengthBits);
    uint32_t minWasted = 8 - entriesPerCode * entryLengthBits;
    uint32_t wasted;

    _encodeDictionary.clear();
    _decodeDictionary.clear();

    if(dictionarySize(_values.size(), elementSize, 1) > maxCache)
      {
	
	// it won't fit in L2 cache
	return -1;
      }

    // prime the loop with one
    _codeLength = 1;

    // find maximum bytes per code
    // solve for the smallest number of wasted bits in our n byte codes
    for(uint32_t i = 2; i <= 4; ++i)
      {
	if(dictionarySize(_values.size(), elementSize, i) <= maxCache)
	  {
		entriesPerCode = floor(i * 8 / entryLengthBits);
		wasted = i * 8 - entriesPerCode * entryLengthBits;
		if(wasted <= minWasted) // less hashmap lookups == better
		  {
		    _codeLength = i;
		    minWasted = wasted;
		  }
	  }
	else
	  {
	    break;
	  }

      }


    // now build all possible combos for _codeLength codes - recurse 
    _entriesPerCode = floor(_codeLength * 8 / entryLengthBits);
    // recursively generate byte-aligned dictionary codes
    std::string blank = ""; // prime the value

    generateAllCodes(_entriesPerCode, 0, blank, entryLengthBits, elementSize);
      
    return 1;
  }


  //iterate over the chunk; find its number of unique values
  // calculate dictionary size for 1, 2, 3, and 4 bytes per codeBlock
  // stop when we find the dict that is larger than L2 cache
  // find the option that wastes the smallest # of bits

  // if returns -1, then the dictionary will not fit in cache
  uint32_t DictionaryEncoding::createDictionary(const uint8_t *src, const size_t elementSize, const size_t nElems)
  {

    uint32_t i;
    uint8_t *readPtr = const_cast<uint8_t *>(src);
    std::string data;
    data.reserve(elementSize);

    
    // entry = indiv dict key for one element
    // code = a block of entries for lookup
    _values.clear();

    for(i = 0; i < nElems; ++i)
      {
	data.assign((char *) readPtr, elementSize);
	
	readPtr += elementSize;
	if(find(_values.begin(), _values.end(), data) == _values.end())
	  {
	    _values.push_back(data);

	  }
      } // find a list of unique elements

    if(generateDictionary(elementSize) == -1) { return 0;   }
    return _values.size();
  }


  // returns number of unique values in our data
  uint32_t DictionaryEncoding::rebuildDictionary(ByteInputItr & in, const size_t elementSize)
  {
    std::string toDecode;
    uint32_t uniqueValues;


    _values.clear();
    if(in.getArray((uint8_t *) &uniqueValues, 4) == -1) { return 0; } 
    toDecode.reserve(elementSize);

    boost::scoped_array<char> valueArr(new char[elementSize]);
    char *value = valueArr.get();
    _values.reserve(uniqueValues);
    for(uint32_t i = 0; i < uniqueValues; ++i)
      {
	if(in.getArray((uint8_t *) value, elementSize) == -1) { return 0; } 
	toDecode.assign(value, elementSize);
	_values.push_back(toDecode);
      }

    generateDictionary(elementSize);

    return uniqueValues;
  }
  
    size_t DictionaryEncoding::compress(void* dst, const ConstChunk& chunk, size_t chunkSize) 
    {
#ifdef FORMAT_SENSITIVE_COMPRESSORS
	uint8_t *src = (uint8_t *)chunk.getData();
	TypeId type = chunk.getAttributeDesc().getType();        
	size_t elementSize = TypeLibrary::getType(type).byteSize();
	size_t nElems = chunkSize / elementSize;

	uint32_t i;
	uint32_t uniqueValues;
	std::string toEncode = "";
	uint32_t code;
	uint8_t *readPtr = (uint8_t *)chunk.getData();


	if(!nElems) {
	  return chunkSize;
	}

        nElems = chunkSize;
        elementSize = 1;

	ByteOutputItr out((uint8_t *) dst, chunkSize-1);

	
	uniqueValues  = createDictionary(src, elementSize, nElems);
	if(uniqueValues == nElems) { 
	  return chunkSize; 
	}

	toEncode.reserve(elementSize);


	// dictionary-specific
        assert(_entriesPerCode);
	uint32_t blocks = floor(nElems / _entriesPerCode);
	uint32_t remainder = nElems % _entriesPerCode;
	size_t blockEntriesSize = _entriesPerCode * elementSize;

	if(uniqueValues == 0) { return chunkSize; }
	if(out.putArray((uint8_t *) &uniqueValues, 4) == -1) {  return chunkSize;}
	// output a list of unique values; we infer their codes by the order that they are read in
	// i.e., first elementSize bytes translate to code 0 and so on


	for(i = 0; i < uniqueValues; ++i)
	  {
	    // put value
	    if(out.putArray((uint8_t *) _values[i].data(), elementSize) == -1) {    return chunkSize; } 
	  }// end dictionary output



	// now output encoded data
	for(i = 0; i < blocks; ++i)
	  {
	    toEncode.assign((char *) readPtr, blockEntriesSize);

	    readPtr += blockEntriesSize;
	    code = _encodeDictionary[toEncode];

	    if(out.putArray((uint8_t *) &code, _codeLength) == -1) {  return chunkSize; } 
	  }
	
	if(remainder)
	  {
	    // output the last few entries -- 
	    toEncode.assign((char *) readPtr, elementSize * remainder);
	    // pad it with _value[0]
	    for(i = 0; i < _entriesPerCode - remainder; ++i)
	      {
		toEncode.append(_values[0]);
	      }
	    code = _encodeDictionary[toEncode];
	    if(out.putArray((uint8_t *) &code, _codeLength) == -1) {   return chunkSize; } 
	  }

	size_t compressed_size = out.close();
	
	return compressed_size;
#else
    return chunkSize;
#endif
    }

  size_t DictionaryEncoding::decompress(void const* src, size_t size, Chunk& chunk)
  {
    size_t chunkSize = chunk.getSize();
    uint8_t* dst = (uint8_t *)chunk.getDataForLoad();
    ByteInputItr in((uint8_t *) src, size);
    uint32_t i;
    uint32_t code = 0;
    std::string value;
    TypeId type = chunk.getAttributeDesc().getType();        
    size_t elementSize = TypeLibrary::getType(type).byteSize();
    size_t nElems = chunkSize / elementSize;

    nElems = chunkSize;
    elementSize = 1;

    if(!nElems)
      {
	return size;
      }

    rebuildDictionary(in, elementSize);
    assert(_entriesPerCode);

    uint32_t blocks = floor(nElems / _entriesPerCode);
    uint32_t remainderSize = (nElems % _entriesPerCode) * elementSize; // in bytes
    size_t blockValueSize = _entriesPerCode * elementSize;
    

    for(i = 0; i < blocks; ++i)
      {
	if(in.getArray((uint8_t *) &code, _codeLength) == -1) {  return 0; } 
	value = _decodeDictionary[code];
	memcpy(dst, value.data(), blockValueSize);
	dst += blockValueSize;
	
      }

    // edge case - last few vals
    if(remainderSize) {
      if(in.getArray((uint8_t *) &code, _codeLength) == -1) {  return 0; } 
      value = _decodeDictionary[code];
      memcpy(dst, value.data(), remainderSize);
      dst += remainderSize;
    }
    


    return dst - (uint8_t *) chunk.getDataForLoad();
	    
  } // end decompress


}
