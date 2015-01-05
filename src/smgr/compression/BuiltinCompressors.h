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

#ifndef BUILTIN_COMPRESSORS_H_
#define BUILTIN_COMPRESSORS_H_

#include "stdint.h"
#include "array/Compressor.h"
#include "ByteInputItr.h"
#include "BitInputItr.h"
#include "ByteOutputItr.h"
#include "BitOutputItr.h"
#include <boost/unordered_map.hpp> 


#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>


namespace scidb 
{
    /**
     * Compressor using Zlib library
     */
    class ZlibCompressor : public Compressor
    {
      public:
        static int compressionLevel; 
        virtual const char* getName() 
        {
            return "zlib";
        }
        virtual size_t compress(void* buf, const ConstChunk& chunk, size_t size);
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk);
        virtual uint16_t getType() const
        {
        	return CompressorFactory::ZLIB_COMPRESSOR;
        }
    };

    /**
     * Compressor using BZlib library
     */
    class BZlibCompressor : public Compressor
    {
      public:
        static int workFactor; // [0..250], default 30
        static int blockSize100k; // [1..9], default 9
        virtual const char* getName() 
        {
            return "bzlib";
        }
        virtual size_t compress(void* buf, const ConstChunk& chunk, size_t size);
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk);
        virtual uint16_t getType() const
        {
        	return CompressorFactory::BZLIB_COMPRESSOR;
        }
    };

    /**
     * Dummy compressor: used for the chunks which do not need compression
     */
    class NoCompression : public Compressor
    {
      public:
        virtual const char* getName() 
        {
            return "no compression";
        }
        virtual size_t compress(void* dst, const ConstChunk& chunk, size_t size);
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk);
        virtual uint16_t getType() const
        {
        	return CompressorFactory::NO_COMPRESSION;
        }
    };

    /**
     * Compressor skipping null values based on bitmap
     */
    class NullFilter : public Compressor
    {
      public:
        virtual const char* getName() 
        {
            return "null filter";
        }
        virtual size_t compress(void* dst, const ConstChunk& chunk, size_t size);
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk);
        virtual uint16_t getType() const
        {
        	return CompressorFactory::NULL_FILTER;
        }
    };

    /** 
     * Compressor for grouping data into runs of the same value
     */

    class RunLengthEncoding : public Compressor
    {
      public:
        RunLengthEncoding() : logger(log4cxx::Logger::getLogger("scidb.smgr.compression.RunLengthEncoding")) {};
        virtual const char* getName() 
        {
            return "rle";
        }
        virtual size_t compress(void* dst, const ConstChunk& chunk, size_t size);
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk);
        virtual uint16_t getType() const
        {
            return CompressorFactory::RUN_LENGTH_ENCODING;
        }
      private:
        log4cxx::LoggerPtr logger;
    };

    class BitmapEncoding : public Compressor
    {
      public:
        virtual const char* getName() 
        {
            return "bitmap encoding";
        }
        virtual size_t compress(void* dst, const ConstChunk& chunk, size_t size);
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk);
        virtual uint16_t getType() const
        {
            return CompressorFactory::BITMAP_ENCODING;
        }

        class Bitmap 
        {
            // helper functions
            void clearBitmapCache();
            void setBit(uint8_t * const bitmap, const uint32_t idx);
            void fillOutput(ByteOutputItr * const target);
            void decodeBitmap(uint8_t const * const baseValue, uint8_t const * const bitmap, uint8_t * const target) ;
            bool getBit(uint8_t byte, uint8_t offset);
            
            // describes and stores the bitmaps
            uint32_t _bitmapElements;
            size_t _elementSize;
            std::vector<uint8_t> _bitSetters;
            std::map<std::string, uint8_t *> _bitmaps;          public:

          public:
            size_t compress(void* dst, const ConstChunk& chunk, size_t size);
            size_t decompress(void const* src, size_t size, Chunk& chunk);

            Bitmap()
            {
                uint8_t bitSetter = 1;
                for(uint32_t i = 0; i < 8; ++i)
                {
                    _bitSetters.push_back(bitSetter  << (7 - i));
                }
                
            }
            ~Bitmap() 
            {
                clearBitmapCache();
            }
        };
    };


    /**
     * Compressor coding out blank lower-end bits
     */
    class NullSuppression : public Compressor
    {
      public:
        virtual const char* getName() 
        {
            return "null suppression";
        }
        NullSuppression();
        virtual size_t compress(void* dst, const ConstChunk& chunk, size_t size);
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk);
        virtual uint16_t getType() const
        {
        	return CompressorFactory::NULL_SUPPRESSION;
        }
      private:
        uint8_t getBytes(uint8_t const * const data, const uint32_t elementSize);  

        // list of all possible prefix combinations precomputed in the constructor for lookup
        std::map<uint8_t, uint64_t> _decode1Bit; // push up to 8 bits-used (1 bit each) values into an uint64; max usage of 64-bit representation
        std::map<uint8_t, uint64_t> _decode2Bits; // push up to 4 bits-used values into an uint64
        std::map<uint8_t, uint64_t> _decode4Bits; // push up to 2 bits-used values into an uint64

        std::map<uint64_t, uint8_t> _encode1Bit; // put in up to 8 values used, get back its bit code
        std::map<uint64_t, uint8_t> _encode2Bits; // put in up to 4 values used, get back its bit cod
        std::map<uint64_t, uint8_t> _encode4Bits; // put in up to 2 values used, get back its bit code
	  
    };


    /**
     * Compressor coding out blank lower-end bits
     */
    class DictionaryEncoding : public Compressor
    {
      public:
	  
        virtual const char* getName() 
        {
            return "dictionary";
        }

        DictionaryEncoding() : logger(log4cxx::Logger::getLogger("scidb.smgr.compression.DictionaryEncoding")) {};
	  


        virtual size_t compress(void* dst, const ConstChunk& chunk, size_t size);
        virtual size_t decompress(void const* src, size_t size, Chunk& chunk);
        virtual uint16_t getType() const
        {
        	return CompressorFactory::DICTIONARY_ENCODING;
        }

        log4cxx::LoggerPtr logger;

      private:
        class Dictionary {
            // list of values --> code
            boost::unordered_map<uint64_t, uint8_t> _encodeDictionary;
            boost::unordered_map<uint8_t, uint64_t> _decodeDictionary;
            log4cxx::LoggerPtr logger;

            uint8_t createDictionary(uint8_t const * const src, const size_t elementSize, const size_t nElems, ByteOutputItr &out);
            
            // returns unique values count - sets up dictionaries for decompression
            uint8_t rebuildDictionary(ByteInputItr & in, const size_t elementSize);

          public:
            Dictionary(log4cxx::LoggerPtr log) : logger(log) {}

            size_t compress(void* dst, const ConstChunk& chunk, size_t size);
            size_t decompress(void const* src, size_t size, Chunk& chunk);
        };        

    };


}

#endif
