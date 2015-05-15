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
#ifndef OP_ARRAY_H_
#define OP_ARRAY_H_

//
//
// BEGIN_COPYRIGHT
// END_COPYRIGHT
//

/**
 *
 *  @file OpArray.h
 *
 *  @brief The implementation of the array delegating all functionality to a
 *         template operator which supplies the vales at a given coordinate.
 *
 */

// de-facto standards
#include <log4cxx/logger.h>

// SciDB APIs
#include <array/DelegateArray.h>        // for SplitArray

using namespace std;            // TODO: fix me
using namespace boost;          // TODO: fix me

namespace scidb
{
static log4cxx::LoggerPtr OP_ARRAY_logger(log4cxx::Logger::getLogger("scidb.linear_algebra.oparray"));   // prefixed since this is in a header



/**
 *  This template class takes a template operand Op_tt, which
 *  represents a function f(coord), and turns it into an Array
 *  which generates dense chunks by calling the function.
 *  When Op_tt::operator()(row, col) is inline, this is extremely efficient.
 *  [There is also an Op_tt::operator()(index) 1D case]
 *
 *  It is implemented as a thin wrapper over SplitArray, by overriding
 *  SplitArray::ArrayIterator::getChunk() to fill the values of the chunk
 *  from the Op_tt instead of from a pointer-to-memory that SplitArray
 *  uses.
 *
 *  See .../scalapackUtil/reformat.hpp for an example of the Op_tt class
 *
 *  It was originally designed to support the reformatting of ScaLAPACK
 *  output to look like an Array.
 *
 *  With some further generalization, this code could be extended to work with
 *  arbitrary array dimensions, be moved into the SciDB tree, and re-base
 *  SplitArray off of it, rather than the other way around
 *
 */
template<class Op_tt>
class OpArray : public SplitArray
{
public:
    enum dbgLevel_e {
        DBG_NONE = 0, 
        DBG_SIMPLE, 
        DBG_DETAIL, 
        DBG_LOOP_SIMPLE,
        DBG_LOOP_DETAIL,
        DBG = DBG_NONE};  // change this one to enable debug traces
                          // of your supplied Op_tt as the chunk is filled

    // could have used SplitArray::ArrayIterator directly, this is just to add debugging
    class ArrayIterator : public SplitArray::ArrayIterator {
    public:
        ArrayIterator(OpArray const& delgate, AttributeID attrID);
        virtual void                operator++();
        virtual ConstChunk const&   getChunk();

        // debugs, same functionality as SplitArray, but adds ability to do debug printing
        // these overrides never needed in production builds
#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
        virtual bool end() {
            bool tmp = SplitArray::ArrayIterator::end();
            if (DBG >= DBG_DETAIL && tmp) {
                std::cerr << "@@ "
                          << _SDbgClass << "::end() returns:" 
                          << tmp << std::endl;
            }
            return tmp;
        }
        virtual const Coordinates& getPosition() {
            const Coordinates& tmp = SplitArray::ArrayIterator::getPosition();
            if (DBG >= DBG_LOOP_DETAIL) {
                std::cerr << "@@@@ " 
                          << _SDbgClass << "::getPos() -> " 
                          << tmp[0] << "," << tmp[1] << std::endl;
            }
            return tmp;
        }
        virtual bool setPosition(const Coordinates & pos) {
            bool tmp = SplitArray::ArrayIterator::setPosition(pos);
            if (DBG >= DBG_LOOP_DETAIL) {
                std::cerr << "@@@@ "
                          << _SDbgClass << "::setPos(" 
                          << pos[0] << "," << pos[1] << ") ->" << tmp << std::endl;
            }
            return tmp;
        }
#endif // SCALAPCK_DEBUG

    protected:
        static const char _SDbgClass[]; //e.g. = "OpArray<Op_tt>::ArrayIterator";
        const OpArray&  _array;
    };
    friend class ArrayIterator;

public:
    OpArray(ArrayDesc const& desc, const boost::shared_array<char>& dummy,
            const Op_tt& op, Coordinates const& from, Coordinates const& till,
            Coordinates const& delta,
            const boost::shared_ptr<scidb::Query>& query);
    virtual ~OpArray();

    virtual Access getSupportedAccess() const
    {
        return Array::MULTI_PASS;
    }

    ArrayIterator*      createArrayIterator(AttributeID id) const;
    private:
    Op_tt               _op;
    Coordinates         _delta; // distance in coordinates between successive chunks on
                                // the same node.  by making this larger than the chunk
                                // size, you can support ScaLAPACK block-cyclic quite
                                // naturally.  When we iterate on a single node, we
                                // iterate from chunk to chunk locally this way.
};

template<class Op_tt>
OpArray<Op_tt>::~OpArray()
{
}

template<class Op_tt>
OpArray<Op_tt>::OpArray(ArrayDesc const& desc,
                        const boost::shared_array<char>& dummy, const Op_tt& op,
                        Coordinates const& from, Coordinates const& till, Coordinates const& delta,
                        const boost::shared_ptr<scidb::Query>& query)
:
    SplitArray(desc, dummy, from, till, query),
    _op(op),
    _delta(delta)
{
    Dimensions const& dims = desc.getDimensions();
    if(OP_ARRAY_logger->isDebugEnabled()) {
        for (size_t i=0; i<dims.size(); i++) {
            LOG4CXX_DEBUG(OP_ARRAY_logger, "OpArray<>::OpArray() dims["<<i<<"] from " << dims[i].getStartMin() << " to " << dims[i].getEndMax());
        }
    }
}

template<class Op_tt>
const char OpArray<Op_tt>::ArrayIterator::_SDbgClass[] = "OpArray<Op_tt>::ArrayIterator::_SDbgClass" ;


template<class Op_tt>
typename OpArray<Op_tt>::ArrayIterator* OpArray<Op_tt>::createArrayIterator(AttributeID id) const
{
    return new ArrayIterator(*this, id);
}

template<class Op_tt>
OpArray<Op_tt>::ArrayIterator::ArrayIterator(OpArray const& delegate,
                                             AttributeID attrID)
:
    SplitArray::ArrayIterator(delegate, attrID),
    _array(delegate)
{
    if(DBG >= DBG_LOOP_DETAIL) {
        std::cerr << "@@@@ OpArray<Op_tt>::ArrayIterator::ArrayIterator()" << std::endl;
        std::cerr << "@@@@                                addr: " << addr.coords[0]
                                                          << ", " << addr.coords[1] << std::endl;
    }
}

template<class Op_tt>
void OpArray<Op_tt>::ArrayIterator::operator++()
{
    if (!hasCurrent)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

    // same as the base class's idiom, but advances by _delta[i] instead of a fixed amount
    // this permits this idiom to advance to the next local block when it is ScaLAPACK-style
    // block-cyclic distribution, because the next logical block is always a constant
    // global offset (delta) away from the current block.

    size_t dim = dims.size()-1;
    // advance the coordinate, are we still in?
    if(DBG >= DBG_LOOP_DETAIL) {
        std::cerr << "OpArray::ArrayIterator::++() addr: " << addr.coords[0] <<","
                                                           << addr.coords[1] << std::endl;
    }

    while ((addr.coords[dim] += _array._delta[dim]) > array.till()[dim]) {
        if (dim == 0) {

            if(DBG >= DBG_LOOP_DETAIL) {
                std::cerr << "OpArray::ArrayIterator::++() returns w/hasCurrent=false"
                          << " addr: " << addr.coords[0]
                          << ", " << addr.coords[1] << std::endl;
            }

            hasCurrent = false; // there's no where to reset to after this
            return;
        }
        addr.coords[dim] = array.from()[dim]; // reset the coordinate to the beginning
        dim -= 1;                             // check the next coordinate
    }

    if(DBG >= DBG_LOOP_DETAIL) {
        std::cerr << "OpArray::ArrayIterator::++() next chunk"
                  << " addr: " << addr.coords[0]
                  << "," << addr.coords[1] << std::endl;
        std::cerr << "OpArray::ArrayIterator::++() returns w/chunkInitialized=false" << std::endl;
    }

    chunkInitialized = false;  // we are on a new chunk
}

template<class Op_tt>
ConstChunk const& OpArray<Op_tt>::ArrayIterator::getChunk()
{
    const char dbgPrefix[]= "OpArray<Op_tt>::ArrayIterator::getChunk() ";
    if(DBG >= DBG_DETAIL) {
        std::cerr << dbgPrefix << "begin" << std::endl;
    }

    if (!hasCurrent) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }

    if (!chunkInitialized) {
        if (DBG >= DBG_DETAIL) {
            std::cerr << dbgPrefix << "START chunk" << std::endl;
            std::cerr << dbgPrefix << " addr.coords.size():"
                      << addr.coords.size() << std::endl;
            std::cerr << dbgPrefix << " dims.size():" << dims.size() << std::endl;
        }
        // chunk is the MemChunk on SplitArray::ArrayIterator
        chunk.initialize(&array, &array.getArrayDesc(), addr, 0);
        
        // duration of getChunk() short enough (<--- DJG this is worrisome!)
        const boost::shared_ptr<scidb::Query>
            localQueryPtr(Query::getValidQueryPtr(_array._query));

        boost::shared_ptr<ChunkIterator> chunkIter =
            chunk.getIterator(localQueryPtr, ChunkIterator::SEQUENTIAL_WRITE);

        Coordinates const& first = chunk.getFirstPosition(false);
        if (DBG >= DBG_DETAIL) {
            std::cerr << dbgPrefix << " FIRST is: " << first << "*******" << std::endl;
        }

        // the last dimension
        const size_t nDims = dims.size();  // OpArray dims
        int64_t colsTillEnd = (_array.till())[nDims-1] - first[nDims-1] + 1 ;
        int64_t chunkCols =  int64_t(dims[nDims-1].getChunkInterval());
        int64_t colCount = std::max(0L, std::min(colsTillEnd, chunkCols));
        if (DBG >= DBG_DETAIL) {
            std::cerr << dbgPrefix << " colCount:" << colCount
                                   << " = max(0,min(colsTillEnd:"<< colsTillEnd
                                   << ", chunkCols:"<< chunkCols
                                   << ")"<< std::endl;
        }

        // temporaries to use the chunkIter->setPostion(), value.setDouble(),
        // and chunkIter->writeItem() API
        Coordinates pos = first;
        Value value;

        if (nDims == 1) {
            if (DBG >= DBG_DETAIL) {
                std::cerr << dbgPrefix << " case nDims 1" << std::endl;
            }

            //
            // note: we are not saying vectors are rows by using "cols" here to step through them
            //       we are just using a variable that is consistent with the "last" dimension, (nDims-1),
            //       as used in the nDims==2 case below, so that we can share some common code
            //       (setup of colsTillEnd, colCount) with that code
            int64_t colEnd = first[0] + colCount ;
            for(int64_t col = first[0]; col < colEnd; col++) {
                double val = _array._op.operator()(col);
                if(DBG >= DBG_DETAIL) {
                    std::cerr << dbgPrefix << "  ["<< col << "]"
                              << " -> val: " << val << std::endl ;
                }
                pos[0] = col ; chunkIter->setPosition(pos);
                value.setDouble(val); chunkIter->writeItem(value);
            }
        } else {
            assert(nDims==2);
            int64_t rowsTillEnd = (_array.till())[0] - first[0] + 1 ;
            int64_t chunkRows =  int64_t(dims[0].getChunkInterval());
            int64_t rowCount = std::max(0L, std::min(rowsTillEnd, chunkRows));
            if(DBG >= DBG_DETAIL) {
                std::cerr << dbgPrefix << " first:" << first[0] << "," << first[1] << std::endl;
                std::cerr << dbgPrefix << " rowCount:" << rowCount
                                       << "= max(0,min(rowsTillEnd:"<< rowsTillEnd
                                                    << ", chunkRows:"<< chunkRows
                                                    << ")" << std::endl;
            }
            int64_t rowEnd = first[0] + rowCount;
            int64_t colEnd = first[1] + colCount;
            // note that SciDB chunks are stored in row-major order, so we
            // iterate columns in the inner loop.
            for(int64_t row = first[0]; row < rowEnd; row++) {
                for(int64_t col = first[1]; col < colEnd; col++) {
                    double val = _array._op.operator()(row, col);
                    if(DBG >= DBG_LOOP_SIMPLE) {
                        std::cerr << dbgPrefix << " ["<< row << "," << col << "] "
                                  << " -> val: " << val << std::endl ;
                    }
                    pos[0] = row; pos[1] = col ; chunkIter->setPosition(pos);
                    value.setDouble(val); chunkIter->writeItem(value);
                }
            }
        }
        chunkIter->flush(); // vital
        chunkInitialized = true;
    }

    if(DBG >= DBG_LOOP_DETAIL) {
        // print the chunk contents as a debug

        // this nasty bit of code was copied from SplitArray and will
        // need to be replaced and refactored when SplitArray is re-based
        // on this class or goes away with the old Linear Algebra operators.
        // In the original it is used for copying data.  Here it is used
        // for printing debug values
        double* dst = reinterpret_cast<double*>(chunk.getData());
        Coordinates const& first = chunk.getFirstPosition(false);
        Coordinates pos=first;
        const size_t nDims = dims.size();
        // dstStrideSize in doubles (not in bytes like original)
        const size_t dstStrideSize = dims[nDims-1].getChunkInterval(); // *attrBitSize>>3
        while (true) {
            size_t offs = 0;
            bool oob = false;
            for (size_t i = 0; i < nDims; i++) {
                offs *= array.size()[i];
                offs += pos[i] - array.from()[i];
                oob |= pos[i] > array.till()[i];
            }
            if (!oob) {
                // len in doubles, (not in bytes like original)
                size_t len = min(size_t(array.till()[nDims-1] - pos[nDims-1] + 1),
                                 size_t(dims[nDims-1].getChunkInterval())); // *attrBitSize >> 3);
                //memcpy(dst, src + (offs*attrBitSize >> 3),
                for(size_t ii=0; ii < len; ii++) {
                    std::cerr << dbgPrefix << "chunk [ii]= " << dst[ii] << std::endl;
                }
            }
            dst += dstStrideSize; // now in doubles, not bytes

            // TODO: the folliwng loop style is taken from the original, I do not care for the use
            // of the goto and think it could probably be refactored
            size_t j = nDims-1;
            while (true) {
                if (j == 0) {
                    goto Done; // evil goto, see comment above
                }
                j -= 1;
                if (++pos[j] >= first[j] + dims[j].getChunkInterval()) {
                    pos[j] = first[j];
                } else {
                break;
                }
            }
        }
    }
Done:

    if(DBG >= DBG_DETAIL) std::cerr << dbgPrefix << "end" << std::endl;

    return chunk;
}

} //namespace

#endif /* DELEGATE_ARRAY_H_ */
