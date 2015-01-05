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





#ifndef DELTA_H_
#define DELTA_H_

#include <exception>

#include "array/Array.h"
#include "smgr/io/InternalStorage.h"

namespace scidb
{

	class DeltaVersionControl : public VersionControl
	{
	public:
		virtual void getVersion(Chunk& dst, ConstChunk const& src, VersionID version);
		virtual bool newVersion(Chunk& dst, ConstChunk const& src, VersionID version, bool append);
	};

	struct DeltaChunkHeader {
		VersionID versionId;
		uint32_t  chunkEnd;  // chunkStart is 1 + the last chunkEnd; or 0
        bool      isSparse;
        bool      isRLE;
	};

	class DeltaBlockHeader {
	public:
		DeltaBlockHeader() {
			typesig = 6445339641633326404LL;  // "DELTArrY"
		}
		uint64_t numChunks;
		uint64_t typesig;

		static const uint64_t TYPESIG = 6445339641633326404LL;
	};


	class InvalidVersionException : public std::exception {
		virtual const char* what() const throw();
	};

	class DeltaBlock
	{
	private:
		SharedBuffer* buf;

		size_t bufSize;
		void *bufData;

		DeltaBlockHeader blockHeader;
		uint64_t chunkTop;
		vector<DeltaChunkHeader> chunkHeaders;

		/**
		 * Format of a delta block in memory:
		 *
		 * Start									End 		Desc.
		 * 0										A			First array, materialized
		 * A+1										B			Second array, delta
		 * B+1										C			Third array, delta
		 * ...
		 * bufSize-8-2*sizeof(DeltaChunkHeader)	bufsize-8-sizeof(DeltaChunkHeader)	Header for second array
		 * bufSize-8-sizeof(DeltaChunkHeader)		bufsize-8	Header for first array
		 * bufSize-8								bufSize		Number of versions in this block
		 *
		 * Where A, B, C, etc can be determined from the DeltaChunkHeader's.
		 */

		void readChunkHeaders();
		void writeChunkHeaders();

		void readBlockHeader();
		void writeBlockHeader();

		void loadBlock();
		void initializeBlock(VersionID version, bool isSparse, bool isRLE);

		void writeHeaders();
		bool isReverseDelta();

	public:
		DeltaBlock(ConstChunk& buf);
		DeltaBlock(ConstChunk& buf, VersionID version);
		DeltaBlock(ConstChunk& buf, VersionID version, bool initialize);

		~DeltaBlock();

		void * allocateChunk(uint64_t size, VersionID versionId, bool isSparse, bool isRLE);

		uint64_t chunkSize(VersionID versionId) throw(InvalidVersionException);
		bool     chunkIsSparse(VersionID versionId) throw(InvalidVersionException);
		bool     chunkIsRLE(VersionID versionId) throw(InvalidVersionException);

		void * chunkData (VersionID versionId) throw(InvalidVersionException);

		uint64_t getNumVersions();
		bool isMaterialized(VersionID versionId);

		uint64_t indexOfVersion(VersionID versionId);
		VersionID versionAtIndex(uint64_t index);
	};

}

#endif // DELTA_H_
