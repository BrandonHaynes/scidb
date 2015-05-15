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
 * @brief Bitmap  encoding implementation
 *
 * @author Jennie Duggan (jennie@cs.brown.edu)
 */

#include "smgr/compression/BuiltinCompressors.h"
#include <math.h>
#include <boost/scoped_array.hpp>


namespace scidb {


    //static log4cxx::LoggerPtr loggerBME(log4cxx::Logger::getLogger("scidb.smgr.compression.BitmapEncoding"));

    void BitmapEncoding::Bitmap::clearBitmapCache()
    {
        std::map<std::string, uint8_t *>::iterator sourcePosition = _bitmaps.begin();
        while(sourcePosition != _bitmaps.end())
        {
            uint8_t *bm = sourcePosition->second;
            assert(bm!=NULL);
            delete [] bm;
            ++sourcePosition;
        }
        _bitmaps.clear();
    }

    void BitmapEncoding::Bitmap::setBit(uint8_t  * const bitmap, const uint32_t idx)
    {
        uint32_t jump;
        //  uint8_t bitOffset;
        uint8_t *bmPosition;
        uint8_t bitSetter = _bitSetters[idx % 8];
        jump = idx / 8;
  
        /**** originally 
              bitOffset = 7 - idx % 8; // left to right, 0-indexed
              bitSetter = bitSetter << bitOffset;
        */

        bmPosition = bitmap + jump;
        *bmPosition = *bmPosition |  bitSetter; // bitwise and to set the bit
  
    }

// bm and # of indexes
    void BitmapEncoding::Bitmap::fillOutput(ByteOutputItr * const target)
    {
        std::map<std::string, uint8_t *>::iterator sourcePosition = _bitmaps.begin();
        uint32_t bytesPerBM = (_bitmapElements + 7) >> 3;


        while(sourcePosition != _bitmaps.end())
        {
            // output the bitmap value
            // error checking done earlier, so we don't need to repeat our check for overflows
            target->putArray((uint8_t *) sourcePosition->first.c_str(), _elementSize);

            uint8_t *currentBM = sourcePosition->second;
            target->putArray(currentBM, bytesPerBM);

            ++sourcePosition;
        }
    }

  
    bool BitmapEncoding::Bitmap::getBit(uint8_t byte, uint8_t offset)
    {
        return (byte & _bitSetters[offset]) > 0;
    }
 
    void BitmapEncoding::Bitmap::decodeBitmap(uint8_t const * const baseValue, uint8_t const * const bitmap, uint8_t * const target) 
    {
        uint32_t i;
        uint8_t *writePtr = target;
        uint8_t *readPtr = const_cast<uint8_t *>(bitmap);
        uint8_t offset = 0;

        for(i = 0; i < _bitmapElements; ++i)
        {

            if(getBit(*readPtr, offset))
            {
                memcpy(writePtr, baseValue, _elementSize);
            }
            writePtr += _elementSize;
      
            ++offset;
            if(offset == 8)
            {
                ++readPtr;
                offset = 0;
            }
       
        }

    }
    /// Compress a source array uint32_to a target array.
    /// Returns the number of bytes in the compressed target array.
    ///

    size_t BitmapEncoding::compress(void* dst, const ConstChunk& chunk, size_t size) 
    {
        return size;
    }

    size_t BitmapEncoding::Bitmap::compress(void* dst, const ConstChunk& chunk, size_t chunkSize) 
    {
        char const* dataSrc = (char const*)chunk.getData();
        TypeId type = chunk.getAttributeDesc().getType();        
        _elementSize = TypeLibrary::getType(type).byteSize();

        /* No more immutable arrays, to keep consistent with old code, always treat data as string
         */
        _bitmapElements = chunkSize;
        _elementSize = 1;

        if(!_bitmapElements) { return chunkSize; }

        char *readPos = const_cast<char *>(dataSrc);
        ByteOutputItr out((uint8_t *) dst, chunkSize-1);
        uint32_t i;
        uint32_t bucketSize = (_bitmapElements + 7) >> 3;
        uint32_t bucketCount = 0;
        std::string key;

        clearBitmapCache();

        // make the key of our hash a string so that 
        // we can compare variable-length element sizes

        size_t bitmapEntryLength = bucketSize + _elementSize;
        assert(bitmapEntryLength);
        uint32_t maxBuckets = floor(chunkSize / bitmapEntryLength);
        if(maxBuckets * bitmapEntryLength == chunkSize)
        {
            // we want to beat the uncompressed case
            --maxBuckets;
        }

        for(i = 0; i < _bitmapElements; ++i)
        { 
            key.clear();

            for(uint32_t j = 0; j < _elementSize; ++j)
            {
                key.push_back(*readPos);
                ++readPos;
            }

            uint8_t *bucket = NULL;
            // check to see if a bucket exists, if so grab and pass on
            std::map<std::string, uint8_t*>::iterator iter  =
                _bitmaps.find(key);

            if(iter == _bitmaps.end() ) {
                ++bucketCount;
                if(bucketCount > maxBuckets)
                {
                    return chunkSize;
                }

                // create a new one             
                bucket = new uint8_t[bucketSize];
                _bitmaps[key] = bucket;
                for(uint32_t k = 0; k < bucketSize; ++k) { *(bucket+k) = 0;} 

            } else {
                bucket = iter->second;
            }
            assert(bucket!=NULL);
            setBit(bucket, i);
        }
        // drop all of bitmaps to dst
        fillOutput(&out);

        size_t compressedSize = out.close();
        return compressedSize;
    }

    size_t BitmapEncoding::decompress(void const* src, size_t size, Chunk& chunk)
    {
        Bitmap bitmap;
        return bitmap.decompress(src, size, chunk);
    }

    size_t BitmapEncoding::Bitmap::decompress(void const* src, size_t size, Chunk& chunk)
    {
        size_t chunkSize = chunk.getSize();
        TypeId type = chunk.getAttributeDesc().getType();        
        _elementSize = TypeLibrary::getType(type).byteSize();

        /* No more immutable arrays, to keep consistent with old code, always treat data as string
         */
        _bitmapElements = chunkSize;
        _elementSize = 1;

        if(!_bitmapElements) { return chunkSize; }

        uint8_t* dst = (uint8_t*)chunk.getDataForLoad();
        ByteInputItr in((uint8_t *)src, size);
        uint32_t bmLength = ceil(_bitmapElements / 8.0);
        uint32_t individualBMLength = bmLength + _elementSize; // value + bm
        assert(individualBMLength);
        uint32_t bitmaps = size / individualBMLength;

        boost::scoped_array<uint8_t> bitmapArr(new uint8_t[bmLength]);
        boost::scoped_array<uint8_t> baseValueArr(new uint8_t[_elementSize]);

        uint8_t *bitmap = bitmapArr.get();
        uint8_t *baseValue = baseValueArr.get();

        for(uint32_t i = 0; i < bitmaps; ++i)
        {
            if(in.getArray(baseValue, _elementSize) == -1) { return 0; }
            if(in.getArray(bitmap, bmLength)== -1) { return 0; } 
            decodeBitmap(baseValue, bitmap, dst);
        }
        return chunkSize;
    }


}
