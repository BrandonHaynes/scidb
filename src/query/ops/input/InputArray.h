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
 * InputArray.h
 *
 *  Created on: Sep 23, 2010
 */
#ifndef INPUT_ARRAY_H
#define INPUT_ARRAY_H

#define __EXTENSIONS__
#define _EXTENSIONS
#define _FILE_OFFSET_BITS 64
#if ! defined(HPUX11_NOT_ITANIUM) && ! defined(L64)
#define _LARGEFILE64_SOURCE 1 // access to files greater than 2Gb in Solaris
#define _LARGE_FILE_API     1 // access to files greater than 2Gb in AIX
#endif

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <string>

#include <boost/enable_shared_from_this.hpp>
#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/MemArray.h>
#include <array/StreamArray.h>

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>

namespace scidb
{
    class ChunkLoader;

    class InputArray : public SinglePassArray, public boost::enable_shared_from_this<InputArray>
    {
        friend class ChunkLoader;

      public:

        static log4cxx::LoggerPtr s_logger;

        /// Constructor
        InputArray(ArrayDesc const& desc,
                   std::string const& format,
                   boost::shared_ptr<Query>& query,
                   bool emptyMode,
                   bool enforceUniqueness,
                   int64_t maxCnvErrors = 0,
                   std::string const& shadowArrayName = string(),
                   bool parallelLoad = false);
        /// Destructor
        virtual ~InputArray();

        void openFile(std::string const& fileName);
        void openString(std::string const& dataString);

        bool inEmptyMode() const { return state == S_Empty; }

        /// Upcalls from the _chunkLoader
        /// @{
        void handleError(Exception const& x,
                         boost::shared_ptr<ChunkIterator> cIter,
                         AttributeID i);
        void completeShadowArrayRow();
        void countCell() { ++nLoadedCells; lastBadAttr = -1; }
        /// @}

        static ArrayDesc generateShadowArraySchema(ArrayDesc const& targetArray,
                                                   std::string const& shadowArrayName);

        /// @returns true iff the named format is supported
        static bool isSupportedFormat(std::string const& format);

    protected:
        /// @see SinglePass::getCurrentRowIndex()
        virtual size_t getCurrentRowIndex() const { return _currChunkIndex; }
        /// @see SinglePass::moveNext()
        virtual bool moveNext(size_t rowIndex);
        /// @see SinglePass::getChunk()
        virtual ConstChunk const& getChunk(AttributeID attr, size_t rowIndex);

    private:

        // Machinery for delayed SG scheduling of the shadow array,
        // which cannot overlap with the scatter/gather of the
        // InputArray itself.
        void sg();
        void redistributeShadowArray(boost::shared_ptr<Query> const& query);
        void scheduleSG(boost::shared_ptr<Query> const& query);
        void resetShadowChunkIterators();

        ChunkLoader*    _chunkLoader;
        size_t          _currChunkIndex;

        Value strVal;
        AttributeID emptyTagAttrID;
        uint64_t nLoadedCells;
        uint64_t nLoadedChunks;
        size_t nErrors;
        size_t maxErrors;
        boost::shared_ptr<Array> shadowArray;
        enum State
        {
            S_Normal,           // We expect to load more chunks
            S_Empty,            // No more chunks, but an SG is needed
            S_Done              // No more chunks, SG scheduled
        };
        State state;
        MemChunk tmpChunk;
        vector< shared_ptr<ArrayIterator> > shadowArrayIterators;
        vector< shared_ptr<ChunkIterator> > shadowChunkIterators;
        size_t nAttrs;
        int lastBadAttr;
        InstanceID myInstanceID;
        bool parallelLoad;
        bool _enforceDataIntegrity;
    };

} //namespace scidb

#endif /* INPUT_ARRAY_H */
