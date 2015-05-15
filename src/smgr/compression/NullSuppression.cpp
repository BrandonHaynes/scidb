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
 * @brief Null suppresssion implementation
 *
 * @author Jennie Duggan (jennie@cs.brown.edu)
 */


#include "smgr/compression/BuiltinCompressors.h"
#include <cmath>


//#include "log4cxx/logger.h"
//#include "log4cxx/basicconfigurator.h"
//#include "log4cxx/helpers/exception.h"


namespace scidb
{
    //static log4cxx::LoggerPtr loggerNS(log4cxx::Logger::getLogger("scidb.smgr.compression.NullSuppression"));
    
    NullSuppression::NullSuppression()
    {
        // generate tables of all prefix combos for prefix bits = 1, 2, 4
        uint64_t bytesUsed; // bytes used decode value
        uint8_t *bytesUsedPtr = (uint8_t *) &bytesUsed;
        uint8_t code, i;
        uint32_t count;

        // populate the lookup tables
        for(count = 0; count < 256; ++count)
        {
            code = (uint8_t) count;
            bytesUsed = 0;

            //64-bit case
            for(i = 0; i < 2; ++i)
            {
                bytesUsedPtr[i] = code >> (i*4) & 15;
            }
            _decode4Bits[code] = bytesUsed;
            _encode4Bits[bytesUsed] = code;

            //32-bit case
            for(i = 0; i < 4; ++i)
            {
                bytesUsedPtr[i] = code >> (i*2) & 3;
            }
            _decode2Bits[code] = bytesUsed;
            _encode2Bits[bytesUsed] = code;

            // 16-bit case
            for(i = 0; i < 8; ++i)
            {
                bytesUsedPtr[i] = code >> i & 1;
            }
            _decode1Bit[code] = bytesUsed;
            _encode1Bit[bytesUsed] = code;

        }
    }

	
    // returns the number of bytes used in a char array of data of size elementSize
    uint8_t NullSuppression::getBytes(uint8_t const * const data, const uint32_t elementSize)
    {
        uint8_t unused = 0;
        uint8_t *readPtr = const_cast<uint8_t *>(data) + elementSize-1;
        uint8_t counter = 0;
        uint8_t bytesUsed;


        // lower-end bits --> higher order bits; readPtr moves backwards
        while(1)
        {
            if(*readPtr == 0)
            {
                ++unused;
            }
            else
            {
                break;
            }
            ++counter;
            --readPtr;
            if(counter == elementSize)
            {
                break;
            }
	
        }

        bytesUsed = elementSize - unused;
        if(bytesUsed == 0)
        {
            ++bytesUsed; // use at least one byte for simplicity
        }
    

        return bytesUsed;
    
    }

  
    size_t NullSuppression::compress(void* dst, const ConstChunk& chunk, size_t chunkSize) 
    {
        return chunkSize;
    }

    size_t NullSuppression::decompress(void const* src, size_t size, Chunk& chunk)
    {

        size_t chunkSize = chunk.getSize();
        TypeId type = chunk.getAttributeDesc().getType();        
        size_t elementSize = TypeLibrary::getType(type).byteSize();
        size_t nElems;

        if(elementSize == 0 || elementSize > 8 || chunk.getAttributeDesc().isNullable())
        {
            nElems = chunkSize;
            elementSize = 1;
        }
        else
        {
            nElems = chunkSize / elementSize;
        }


        uint8_t* writePtr = (uint8_t*)chunk.getDataForLoad();

        uint32_t codeLength = log2(elementSize);
        codeLength = (codeLength == 0) ? 1 : codeLength;

        uint32_t blockLength = floor(8 / codeLength);
        blockLength = (blockLength == 0) ? 1 : blockLength;

        uint32_t blocks = floor(nElems / blockLength);
        uint32_t endElements = nElems - blocks * blockLength;
        uint8_t *readPtr = (uint8_t *) src;
    
        uint32_t i, j, k;


        uint8_t *blockCode;

        uint64_t bytesUsed;
        uint8_t *bytesUsedPtr = (uint8_t *) &bytesUsed;
        uint8_t remain;

        std::map<uint8_t, uint64_t> *decoder;

        if(!nElems)
        {
            memcpy(writePtr, readPtr, size);
            return chunkSize;
        }

        if(elementSize == 2)
        {
            decoder = &_decode1Bit;
        }
        else if(elementSize == 4)
        {
            decoder = &_decode2Bits;
        }
        else if(elementSize == 8)
        {
            decoder = &_decode4Bits;
        }
        else
        {
            // not supported data type -- we should never get here
            memcpy(writePtr, readPtr, chunkSize);
            return chunkSize;
        }    


        if(size == chunkSize)
        {
            memcpy(writePtr, readPtr, chunkSize);
            return chunkSize;
        }



        for(i = 0; i < blocks; ++i)
        {
            blockCode = readPtr;
            ++readPtr;
            bytesUsed = (*decoder)[*blockCode];
            for(j = 0; j < blockLength; ++j)
            {

                memcpy(writePtr, readPtr, bytesUsedPtr[j]);
                readPtr += bytesUsedPtr[j];
                writePtr += bytesUsedPtr[j];
                remain = elementSize - bytesUsedPtr[j];
                for(k = 0; k < remain; ++k)
                {
                    *writePtr = 0;
                    ++writePtr;
                } // null the ones that were encoded out
            }// end block
        } // end block iterator
    

        blockCode = readPtr;
        ++readPtr;
        bytesUsed = (*decoder)[*blockCode];

        // cover remainder case
        for(i = 0; i < endElements; ++i)
        {
            memcpy(writePtr, readPtr, bytesUsedPtr[i]);
            readPtr += bytesUsedPtr[i];
            writePtr += bytesUsedPtr[i];

            remain = elementSize - bytesUsedPtr[i];
            for(j = 0; j < remain; ++j)
            {
                *writePtr = 0;
                ++writePtr;
            }
        }

        return chunkSize;
    }

}
