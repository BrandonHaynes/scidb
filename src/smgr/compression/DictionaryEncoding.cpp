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
#include "system/Sysinfo.h"
#include "smgr/compression/BuiltinCompressors.h"
#include <cmath>


namespace scidb { 
    uint8_t DictionaryEncoding::Dictionary::createDictionary(uint8_t const * const src, const size_t elementSize, const size_t nElems, ByteOutputItr &out)
    {

        uint32_t i;
        uint8_t count = 0;
        uint64_t value = 0;
        uint8_t *readPtr = const_cast<uint8_t *>(src);
        uint8_t *uniquePtr = out.getWritePtr();
        if(out.put(count) == -1) { return 0; }

        size_t maxCache = Sysinfo::getCPUCacheSize(Sysinfo::CPU_CACHE_L2 | Sysinfo::CPU_CACHE_L3) * 3 / 4;
        size_t maxElements = maxCache / 17; // 17 bytes = approx size of one table element, 8 for key, 1 for code, one 8-byte pointer to the code
        if(maxElements > 255) { maxElements = 255; }


        _encodeDictionary.clear();
  
        for(i = 0; i < nElems; ++i)
        {
            memcpy((uint8_t *) &value, readPtr, elementSize);

            if(_encodeDictionary.find(value) == _encodeDictionary.end())
            {
                _encodeDictionary[value] = count;
                ++count;
                if(count == 255) {  return 0; }
                if(out.putArray((uint8_t *) &value, elementSize) == -1) {   return 0; } 
            }
            readPtr += elementSize;
        }

        *uniquePtr = count;
        return count;
    }


    uint8_t DictionaryEncoding::Dictionary::rebuildDictionary(ByteInputItr & in, const size_t elementSize)
    {
        uint8_t i;
        uint8_t uniques;
        uint64_t value = 0;


        if(in.get(uniques) == -1) { return 0; }
  
        _decodeDictionary.clear();

        for(i = 0; i < uniques; ++i)
        {
            if(in.getArray((uint8_t *) &value, elementSize) == -1) { return 0; } 
            _decodeDictionary[i] = value;
        }
  
        return uniques;
    }


    size_t DictionaryEncoding::compress(void* dst, const ConstChunk& chunk, size_t size)
    {
#ifdef FORMAT_SENSITIVE_COMPRESSORS
        Dictionary dict(logger);
        return dict.compress(dst, chunk, size);
#else
        return size;
#endif
    }

    size_t DictionaryEncoding::Dictionary::compress(void* dst, const ConstChunk& chunk, size_t chunkSize)
    {
        uint8_t *readPtr = (uint8_t *)chunk.getData();
        TypeId type = chunk.getAttributeDesc().getType();        
        size_t elementSize = TypeLibrary::getType(type).byteSize();
        size_t nElems;

        /* No more immutable arrays, to keep consistent with old code, always treat data as string
         */
        nElems = chunkSize;
        elementSize = 1;

        size_t i;
        uint64_t value = 0;
        uint8_t code = 0;
        ByteOutputItr out((uint8_t *) dst, chunkSize - 1);
        BitOutputItr outBits(&out);

        uint32_t uniques = (uint32_t) createDictionary(readPtr, elementSize, nElems, out);
  
        size_t codeLength;
        uniques <= 2 ? codeLength = 1 : codeLength = ceil(log2(uniques-1)) + 1;  // 0-indexed, so values span from 0...uniques-1, log is 0-based, so bring it back to 1...n bits
  
        // project size and terminate if it will be too large
        size_t codesSize = (nElems * codeLength + 7) >> 3;
        size_t totalCompressed = 1 + uniques * elementSize + codesSize;

        if(totalCompressed*2 >= chunkSize) // if we can't get at least 2:1 it is not worth doing
        {
            return chunkSize;
        }



        if(!nElems || !uniques) 
        {
            return chunkSize;
        }

        for(i = 0; i < nElems; ++i)
        {
            memcpy((uint8_t *) &value, readPtr, elementSize);
            code = _encodeDictionary[value];
            outBits.put(code, codeLength);
            readPtr += elementSize;
        }
  
        outBits.flush();
        size_t compressedSize = out.close();

  
        return compressedSize;

    }


    size_t DictionaryEncoding::decompress(void const* src, size_t size, Chunk& chunk)
    {
        Dictionary dict(logger);
        return dict.decompress(src, size, chunk);
    }

    size_t DictionaryEncoding::Dictionary::decompress(void const* src, size_t size, Chunk& chunk)
    {
        size_t chunkSize = chunk.getSize();
        uint8_t* writePtr = (uint8_t *)chunk.getDataForLoad();
        ByteInputItr in((uint8_t *) src, size);
        uint32_t i;
        uint8_t code = 0;
        uint64_t value = 0;
        TypeId type = chunk.getAttributeDesc().getType();        
        size_t elementSize = TypeLibrary::getType(type).byteSize();
        size_t nElems;

        /* No more immutable arrays, to keep consistent with old code, always treat data as string
         */
        nElems = chunkSize;
        elementSize = 1;

        uint32_t uniques = (uint32_t ) rebuildDictionary(in, elementSize);
        BitInputItr inBits(&in);
        size_t codeLength;
        uniques <= 2 ? codeLength = 1 : codeLength = ceil(log2(uniques-1))+1;

        size_t codesSize = (nElems * codeLength + 7) >> 3;
        size_t totalCompressed = 1 + uniques * elementSize + codesSize;

        if(totalCompressed != size || !uniques)  // malformed compression, don't work on it
        {
            return 0;
        }

        for(i = 0; i < nElems; ++i)
        {
            inBits.get(code, codeLength);
            value = _decodeDictionary[code];
            memcpy(writePtr, (uint8_t *) &value, elementSize);
            writePtr += elementSize;
        }


        return chunkSize;
    }
 
}
