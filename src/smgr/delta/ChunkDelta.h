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


#ifndef CHUNKDELTA_H_
#define CHUNKDELTA_H_

#include <string.h>

#include <vector>
using std::vector;

#include <exception>

#include <sstream>
using std::stringstream;

#include "array/Array.h"
#include "array/MemArray.h"
#include "array/Metadata.h"
#include "query/TypeSystem.h"

#include "bsdiff/bsdiff.h"

namespace scidb
{

    TypeId getTypeIdForIntSize(int size);

	struct SubtractionDeltaHeader
	{
		uint64_t sparseDataLength;
		uint64_t denseDataLength;
		uint8_t denseBitDepth;
	};

	enum DeltaType {
		SUBTRACTIVE = 0x00,
		BSDIFF = 0xFF
	};

	class InvalidDeltaException : public std::exception
	{
	private:
		int _excNum;

	public:
		InvalidDeltaException(int excNum);
		const char* what() const throw();
	};

	class ChunkDelta
	{
	protected:
		vector<ChunkDelta*> deltasToApply;

		void * fromDeltaBuffer;
		off_t fromDeltaBufSize;
		bool needToFreeBuffer;

		void * deltaData;
		off_t deltaBufSize;
		uint8_t type;

		bool validDelta;

		const static Value ValueDifference(const Value& v1, const Value& v2);

		void writeDeltaData(const uint8_t bitDepth, const MemChunk& sparseData, const MemChunk& denseData);
		static MemChunk& subtractChunks(MemChunk& deltaChunk, const ConstChunk& srcChunk, const ConstChunk& targetChunk);

		/*
		 * First byte of a delta is "type".  Current type options are:
		 *
		 * 0x00 -- Subtraction-based
		 * 0xFF -- BSDIff-based
		 *
		 */

		/*
		 * Layout of a subtraction-based delta:
		 *
		 * Start		End
		 * 0			1			Type
		 * 1			9			Sparse Delta Length (SLen)
		 * 10			17			Dense Delta Length (DLen)
		 * 18			25			Extra Data Length (Len)
		 * 26			26+SLen		Sparse Delta Data (MemChunk format)
		 * 26+SLen		26+[SD]Len	Dense Delta Data (MemChunk format)
		 * 26+[SD]Len	26+[SDE]Len	Extra Data (data from append's)
		 *
		 * If a length is 0, that component does not exist.
		 * It should be treated as if each cell in it has value 0.
		 *
		 */

		/*
		 * Layout of a BSDiff-based delta:
		 *
		 * Start	End
		 * 0		1			Type
		 * 1		9			Length
		 * 9		9+Length	BSDiff data
		 *
		 */

		void createDelta_Subtractive(const ConstChunk& srcChunk, const ConstChunk& targetChunk);
		void createDelta_BSDiff(const ConstChunk& srcChunk, const ConstChunk& targetChunk);

		void applyDeltas_Subtractive(const ConstChunk& srcChunk, SharedBuffer& out);
		void applyDeltas_BSDiff(const ConstChunk& srcChunk, SharedBuffer& out);

	public:
		ChunkDelta(void * _fromDeltaBuffer, off_t _bufSize);

		ChunkDelta(const ConstChunk& srcChunk, const ConstChunk& targetChunk);

		~ChunkDelta();

		bool isValidDelta();

		bool applyDelta(const ConstChunk& srcChunk, SharedBuffer& out);
		void pushDelta(ChunkDelta& d);

		void* getData();
		size_t getSize();
	};

}


#endif // CHUNKDELTA_H_
