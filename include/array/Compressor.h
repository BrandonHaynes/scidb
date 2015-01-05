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


/*
 * Compressor.h
 *
 *  Created on: 07.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Compressor interface
 */

#ifndef COMPRESSOR_H_
#define COMPRESSOR_H_

#include <vector>
#include "array/Array.h"

namespace scidb 
{
    /**
     * Compressor interface
     */
    class Compressor 
    { 
      public:
        /**
         * Compress data. If compressed size is greater ot equal than original size, then 
         * no compression should be perfromed and original size should be returned
         * @param buf buffer for compressed data, it should be at least "chunk->getSize()" byte length
         * @param chunk compressed chunk
         * @param size size of data to be compressed (can be smaller than chunk.getSize())
         * @return size of compressed data
         */
        virtual size_t compress(void* buf, const ConstChunk& chunk, size_t size) = 0;

        inline size_t compress(void* buf, const ConstChunk& chunk) {
            return compress(buf, chunk, chunk.getSize());
        }
        
        /**
         * Decompress data.
         * @param src source buffer with compressed data
         * @param size compressed size
         * @param chunk decompressed chunk
         * @return decompressed size (0 should be returned in case of error)
         */
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk) = 0;

        /**
         * Get compressor's name
         */
        virtual const char* getName() = 0;

        /**
         * Get compressor's type
         */
        virtual uint16_t getType() const = 0;

        /**
         * Destructor
         */
        virtual ~Compressor() {}
    };

    /**
     * Collection of all available compressors
     */
    class CompressorFactory 
    {
        std::vector<Compressor*> compressors;
        static CompressorFactory instance;
      public:
        CompressorFactory();

        enum PredefinedCompressors { 
            NO_COMPRESSION,
            NULL_FILTER,
            RUN_LENGTH_ENCODING,
            BITMAP_ENCODING,
            NULL_SUPPRESSION,
            DICTIONARY_ENCODING,
            ZLIB_COMPRESSOR,
            BZLIB_COMPRESSOR,
            USER_DEFINED_COMPRESSOR
        };
        
        void registerCompressor(Compressor* compressor);

        static const CompressorFactory& getInstance()
        {
            return instance;
        }

        const std::vector<Compressor*>& getCompressors() const
        {
            return compressors;
        }
        ~CompressorFactory();
    };

}

#endif
