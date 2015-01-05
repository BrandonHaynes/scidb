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


#include <stdint.h>
#include <string.h>
#include <assert.h>

#include <vector>
using std::vector;

#include <exception>
using std::exception;

#include <algorithm>
using std::lower_bound;

#include <boost/shared_ptr.hpp>
using boost::shared_ptr;

#include "smgr/delta/Delta.h"
#include "smgr/delta/ChunkDelta.h"
#include "array/Array.h"
#include "smgr/io/InternalStorage.h"
#include "system/SystemCatalog.h"

/*
 * SciDB can do either forward or reverse delta's.
 * "Forward" means that the oldest version within a chunk is materialized and
 * subsequent versions are deltas.
 * "Reverse" means that the newest version within a chunk is materialized and
 * prior versions are deltas.
 * Reverse deltas tend to give better performance when selecting recent versions,
 * since the delta chain doesn't need to be re-materialized; but they require
 * rewriting the whole chunk just to add a small delta.
 * To disable them and revert to Forward deltas, "#define SCIDB_FORWARD_DELTAS".
 */
//#define SCIDB_FORWARD_DELTAS

/*
 * Also, SciDB can be run without delta-compression support at all.
 * To disable it, "#define SCIDB_NO_DELTA_COMPRESSION".
 */
//#define SCIDB_NO_DELTA_COMPRESSION

namespace scidb
{

void DeltaVersionControl::getVersion(Chunk& dst, ConstChunk const& src, VersionID version)
{
	DeltaBlock block((ConstChunk&)src);

    dst.setSparse(block.chunkIsSparse(version));
    dst.setRLE(block.chunkIsRLE(version));
    
	if (block.isMaterialized(version)) {
		uint64_t size = block.chunkSize(version);
		dst.allocate(size);
		memcpy(dst.getData(), block.chunkData(version), size);
	}
	else
	{
		uint64_t targetVer = block.indexOfVersion(version);
		if (targetVer == (uint64_t)-1) {
			throw InvalidVersionException();
		}

		vector< shared_ptr<ChunkDelta> > deltas;
		deltas.reserve(targetVer);

		for (uint64_t i = 1; i <= targetVer; i++) {
			VersionID v = block.versionAtIndex(i);
			shared_ptr<ChunkDelta> delta(new ChunkDelta(block.chunkData(v), (off_t)block.chunkSize(v)));
			deltas.push_back(delta);
		}

		for (uint64_t i = 1; i < deltas.size(); i++) {
			deltas[0]->pushDelta(*deltas[i]);
		}

		dst.allocate( block.chunkSize(block.versionAtIndex(0)) );
		deltas[0]->applyDelta(src, dst);
	}
}

bool DeltaVersionControl::newVersion(Chunk& dst, ConstChunk const& src, VersionID version, bool append)
{
//	if (append) {
    // K&K: ????
    // VersionID currVersion = SystemCatalog::getInstance()->getLastVersion(src.getArrayDesc().getId());
    VersionID currVersion = version-1;
    DeltaBlock block(dst, currVersion, !append);

    uint64_t targetVer = block.getNumVersions();
    block.versionAtIndex(0);
    
    MemChunk currLatestVersion;
    MemChunk dstChunk;
    dstChunk.allocate(dst.getSize());
    memcpy(dstChunk.getData(), dst.getData(), dst.getSize());
    dstChunk.initialize(dst);
    currLatestVersion.initialize(dst);

#ifdef SCIDB_FORWARD_DELTAS
    DeltaVersionControl::getVersion(currLatestVersion, dstChunk, block.versionAtIndex(targetVer - 1));
        
    ChunkDelta delta(currLatestVersion, src);
    if (!delta.isValidDelta() || delta.getSize() > src.getSize()) {
        return false;
    }
    
    memcpy(block.allocateChunk(delta.getSize(), version, src.isSparse(), src.isRLE()), delta.getData(), delta.getSize());
    
    return true;
#else // #ifdef SCIDB_FORWARD_DELTAS
    DeltaVersionControl::getVersion(currLatestVersion, dstChunk, block.versionAtIndex(0));
    
    ChunkDelta delta(src, currLatestVersion);
    if (!delta.isValidDelta() || delta.getSize() > src.getSize()) {
        return false;
    }

    // And now the fun begins.
    // With reverse deltas, we have to rebuild the whole chunk.
    MemChunk newDst;
    
    // Add the new most-recent-version materialized
    newDst.allocate(src.getSize());
    newDst.setSparse(src.isSparse());
    newDst.setRLE(src.isRLE());
    memcpy(newDst.getData(), src.getData(), newDst.getSize());
    DeltaBlock newBlock(newDst, version, true);
    VersionID versionAtIndex;
    
    // Add the previous most-recent-version as a delta
    versionAtIndex = block.versionAtIndex(0);
    memcpy(newBlock.allocateChunk(delta.getSize(), versionAtIndex, block.chunkIsSparse(versionAtIndex), block.chunkIsRLE(versionAtIndex)),
           delta.getData(),
           delta.getSize());
    
    // And re-add all of the old delta's.
    for (uint64_t i = 1; i < targetVer; i++) {
        versionAtIndex = block.versionAtIndex(i);
        memcpy(newBlock.allocateChunk(block.chunkSize(versionAtIndex), versionAtIndex, block.chunkIsSparse(versionAtIndex), block.chunkIsRLE(versionAtIndex)),
               block.chunkData(versionAtIndex),
               block.chunkSize(versionAtIndex));
    }
    
    // Now overwrite the existing buffer with this buffer's data
    // Note that it's illegal to reallocate dst while it's used by block, so
    // we can't use block after this point.
    // Should probably enforce this with scoping...
    dst.allocate(newDst.getSize());
    memcpy(dst.getData(), newDst.getData(), newDst.getSize());
    
    return true;
#endif 
}

// A comparator used for searching through a sorted list of versions.
// Intended for the STL binary-search algorithms.
inline bool VersionComparator(DeltaChunkHeader a, DeltaChunkHeader b) { return (a.versionId < b.versionId); }

bool DeltaBlock::isReverseDelta() {
	return (chunkHeaders.size() > 1 && chunkHeaders.front().versionId > chunkHeaders.back().versionId);
}

uint64_t DeltaBlock::indexOfVersion(uint64_t versionId)
{
	DeltaChunkHeader versionHeader;
	versionHeader.versionId = versionId;
	if (isReverseDelta()) {
		vector<DeltaChunkHeader>::reverse_iterator i;
		i = lower_bound(chunkHeaders.rbegin(), chunkHeaders.rend(), versionHeader, &VersionComparator);
		if (i != chunkHeaders.rend() && (*i).versionId == versionId) {
			return (chunkHeaders.rend() - i) - 1;
		} else {
			return (uint64_t)-1;
		}
	} else {
		vector<DeltaChunkHeader>::iterator i;
		i = lower_bound(chunkHeaders.begin(), chunkHeaders.end(), versionHeader, &VersionComparator);
		if (i != chunkHeaders.end() && (*i).versionId == versionId) {
			return (i - chunkHeaders.begin());
		} else {
			return (uint64_t)-1;
		}
	}
}

uint64_t DeltaBlock::versionAtIndex(uint64_t index) {
	return chunkHeaders[index].versionId;
}

const char* InvalidVersionException::what() const throw() {
	return "Queried a chunk for a version not in that chunk";
}

void DeltaBlock::readChunkHeaders() {
	void *headerPtr = (uint8_t*)bufData + bufSize - sizeof(DeltaBlockHeader);

	chunkHeaders.resize(blockHeader.numChunks);

	// This can't be done in one memcpy() because the on-disk format
	// does not guarantee any particular byte alignment and vector<> does.
	for (uint64_t v = 0; v < blockHeader.numChunks; v++) {
		headerPtr = (uint8_t*)headerPtr - sizeof(DeltaChunkHeader);
		memcpy(&chunkHeaders[v], headerPtr, sizeof(DeltaChunkHeader));
	}
}

void DeltaBlock::writeChunkHeaders() {
	void *headerPtr = (uint8_t*)bufData + bufSize - sizeof(DeltaBlockHeader);

	// This can't be done in one memcpy() because the on-disk format
	// does not guarantee any particular byte alignment and vector<> does.
	for (uint64_t v = 0; v < chunkHeaders.size(); v++) {
		headerPtr = (uint8_t*)headerPtr - sizeof(DeltaChunkHeader);
		memcpy(headerPtr, &chunkHeaders[v], sizeof(DeltaChunkHeader));
	}
}

void DeltaBlock::readBlockHeader() {
	memcpy(&blockHeader, (uint8_t*)bufData + bufSize - sizeof(DeltaBlockHeader), sizeof(DeltaBlockHeader));
	assert(blockHeader.typesig == DeltaBlockHeader::TYPESIG);
}
void DeltaBlock::writeBlockHeader() {
	memcpy((uint8_t*)bufData + bufSize - sizeof(DeltaBlockHeader), &blockHeader, sizeof(DeltaBlockHeader));
}

void DeltaBlock::loadBlock() {
	readBlockHeader();
	readChunkHeaders();
	if (blockHeader.numChunks > 0) {
		chunkTop = chunkHeaders.back().chunkEnd;
	} else {
		chunkTop = 0;
	}
}

void DeltaBlock::initializeBlock(VersionID newVersion, bool isSparse, bool isRLE) {
	blockHeader.numChunks = 1;
	chunkTop = 0;
	DeltaChunkHeader h;
	h.versionId = newVersion;
	h.chunkEnd = buf->getSize();
    h.isSparse = isSparse;
    h.isRLE = isRLE;
	chunkHeaders.push_back(h);
	buf->reallocate(buf->getSize()+sizeof(DeltaBlockHeader)+sizeof(DeltaChunkHeader));
	bufSize = buf->getSize();
	bufData = buf->getData();
	writeHeaders();
	assert(blockHeader.numChunks == 1);
}

void DeltaBlock::writeHeaders() {
	writeBlockHeader();
	writeChunkHeaders();
}

DeltaBlock::DeltaBlock(ConstChunk& chunk, VersionID newVersion, bool initialize) : buf(&chunk) {
	chunk.pin();

	if (initialize) {
		initializeBlock(newVersion, chunk.isSparse(), chunk.isRLE());
	} else {
		bufSize = chunk.getSize();
		bufData = chunk.getData();
	}

	loadBlock();
}

DeltaBlock::DeltaBlock(ConstChunk& chunk, VersionID newVersion) : buf(&chunk) {
	chunk.pin();
	initializeBlock(newVersion, chunk.isSparse(), chunk.isRLE());
	loadBlock();
}

DeltaBlock::DeltaBlock(ConstChunk& chunk) : buf(&chunk) {
	chunk.pin();

	bufSize = chunk.getSize();
	bufData = chunk.getData();

	loadBlock();
}

DeltaBlock::~DeltaBlock() {
	buf->unPin();
}

void * DeltaBlock::allocateChunk(uint64_t size, uint64_t versionId, bool isSparse, bool isRLE) {
	buf->reallocate(buf->getSize() + size + sizeof(DeltaChunkHeader));
	bufData = buf->getData();
	bufSize = buf->getSize();

	// We're about to update chunkTop.  Save some work; the old top is the new bottom of the array.
	void * chunkPtr = (uint8_t*)bufData + chunkTop;

	chunkTop += size;
	blockHeader.numChunks ++;
	DeltaChunkHeader h;
	h.versionId = versionId;
	h.chunkEnd = chunkTop;
    h.isSparse = isSparse;
    h.isRLE = isRLE;
	chunkHeaders.push_back(h);
	writeHeaders();

	return chunkPtr;
}

uint64_t DeltaBlock::chunkSize(uint64_t versionNumber) throw(InvalidVersionException) {
	uint64_t index = indexOfVersion(versionNumber);

	if (index == (uint64_t)-1) {
		throw InvalidVersionException();
	}

	if (index == 0) {
		return chunkHeaders[0].chunkEnd;
	} else {
		return (chunkHeaders[index].chunkEnd - chunkHeaders[index-1].chunkEnd);
	}
}

bool DeltaBlock::chunkIsSparse(uint64_t versionNumber) throw(InvalidVersionException) {
	uint64_t index = indexOfVersion(versionNumber);

	if (index == (uint64_t)-1) {
		throw InvalidVersionException();
	}
    return chunkHeaders[index].isSparse;
}

bool DeltaBlock::chunkIsRLE(uint64_t versionNumber) throw(InvalidVersionException) {
	uint64_t index = indexOfVersion(versionNumber);

	if (index == (uint64_t)-1) {
		throw InvalidVersionException();
	}
    return chunkHeaders[index].isRLE;
}

void * DeltaBlock::chunkData (uint64_t versionNumber) throw(InvalidVersionException) {
	uint64_t index = indexOfVersion(versionNumber);

	if (index == (uint64_t)-1) {
		throw InvalidVersionException();
	}

	if (index == 0) {
		return bufData;
	} else {
		return (uint8_t*)bufData + chunkHeaders[index-1].chunkEnd;
	}
}

uint64_t DeltaBlock::getNumVersions()
{
	return blockHeader.numChunks;
}

bool DeltaBlock::isMaterialized(uint64_t versionId)
{
	return (chunkHeaders.size() > 0 && chunkHeaders[0].versionId == versionId);
}

}
