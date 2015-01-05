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
 * @brief Compressors implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <string.h>
#include <assert.h>
#include <zlib.h>
#include <bzlib.h>

#include "smgr/compression/BuiltinCompressors.h"

//#include "log4cxx/logger.h"
//#include "log4cxx/basicconfigurator.h"
//#include "log4cxx/helpers/exception.h"

namespace scidb
{
  //static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr.compression.Compressor"));
    CompressorFactory CompressorFactory::instance;
    
    void CompressorFactory::registerCompressor(Compressor* compressor)
    {
        compressors.push_back(compressor);
    }

    CompressorFactory::CompressorFactory()
    {
        compressors.push_back(new NoCompression());
        compressors.push_back(new NullFilter());
        compressors.push_back(new RunLengthEncoding());
        compressors.push_back(new BitmapEncoding());
        compressors.push_back(new NullSuppression());
        compressors.push_back(new DictionaryEncoding());
        compressors.push_back(new ZlibCompressor());
        compressors.push_back(new BZlibCompressor());
    }

    CompressorFactory::~CompressorFactory()
    {
        for (size_t i = compressors.size() - 1; i; i--) {
            delete compressors[i];
        }
    }

    size_t NoCompression::compress(void* dst, const ConstChunk& chunk, size_t size)
    {
        return size;
    }
    
    size_t NoCompression::decompress(void const* src, size_t size, Chunk& chunk) 
    {
        memcpy(chunk.getDataForLoad(), src, size);
        return size;
    }
    
    int ZlibCompressor::compressionLevel = Z_DEFAULT_COMPRESSION;
    
    size_t ZlibCompressor::compress(void* dst, const ConstChunk& chunk, size_t size) 
    {
        uLongf dstLen = size;
        int rc = compress2((Bytef*)dst, &dstLen, (Bytef*)chunk.getData(), size, compressionLevel);
        return rc == Z_OK ? dstLen : size;
    }
    
    size_t ZlibCompressor::decompress(void const* src, size_t size, Chunk& chunk) 
    {
        uLongf dstLen = chunk.getSize();
        int rc = uncompress((Bytef*)chunk.getDataForLoad(), &dstLen, (Bytef*)src, size);
        return rc == Z_OK ? dstLen : 0;
    }

    int BZlibCompressor::blockSize100k = 4;
    int BZlibCompressor::workFactor = 9; 
 
    size_t BZlibCompressor::compress(void* dst, const ConstChunk& chunk, size_t size) 
    {
        unsigned int dstLen = size;
        int rc = BZ2_bzBuffToBuffCompress((char*)dst, &dstLen, (char*)chunk.getData(), size, blockSize100k, 0, workFactor);
        return rc == BZ_OK ? dstLen : size;
    }
    
    size_t BZlibCompressor::decompress(void const* src, size_t size, Chunk& chunk) 
    {
        unsigned int dstLen = chunk.getSize();
        int rc = BZ2_bzBuffToBuffDecompress((char*)chunk.getDataForLoad(), &dstLen, (char*)src, size, 0, 0);
        return rc == BZ_OK ? dstLen : 0;
    }

    size_t NullFilter::compress(void* dst, const ConstChunk& chunk, size_t size) 
    {
        return size;
    }

    size_t NullFilter::decompress(void const* src, size_t size, Chunk& chunk) 
    {
      
      return 0;
    }
}
    



