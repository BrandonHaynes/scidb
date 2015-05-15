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
 * PhysicalUnpack.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 *      Author: poliocough@gmail.com
 */

#include <boost/scope_exit.hpp>

#include <query/Operator.h>
#include <util/Network.h>
#include <array/Metadata.h>

using namespace std;
using namespace boost;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.unpack"));

namespace scidb
{

/**
 * A simple marshallable struct combining the coordinates of a chunk, the number of elements the chunk contains and the
 * starting position of the chunk in the output array.
 */
struct UnpackChunkAddress
{
    /**
     * Position of the input chunk.
     */
    Coordinates inputChunkPos;

    /**
     * Number of elements in the chunk.
     */
    size_t elementCount;

    /**
     * The starting position of the chunk in the output 1D array.
     */
    mutable Coordinate outputPos;

    /**
     * Compute the marshalled size (in bytes) of any UnpackChunkAddress for a given
     * number of dimensions.
     * @param[in] ndDims the number of dimensions in the input array
     */
    inline static size_t marshalledSize(size_t const nDims)
    {
        return sizeof(Coordinate) * (nDims + 1) + sizeof(size_t);
    }

    /**
     * Marshall this into a buffer. The structure will occupy exactly marshalledSize(nDims) bytes.
     * @param[in|out] buf a pointer to memory where to start writing the structure. Better not be null and have enough size to hold all data
     * @param[in] nDims the number of dimensions; provided externally for performance
     * @return ((uint8_t *)buf) + marshalledSize(nDims)
     */
    inline void* marshall(void *buf, size_t const nDims) const
    {
        Coordinate *cPtr = reinterpret_cast<Coordinate *>(buf);
        for(size_t i=0; i<nDims; i++)
        {
            *cPtr = inputChunkPos[i];
            ++cPtr;
        }

        size_t *sPtr = reinterpret_cast<size_t *>(cPtr);
        *sPtr = elementCount;
        ++sPtr;

        cPtr = reinterpret_cast<Coordinate *>(sPtr);
        *cPtr = outputPos;
        ++cPtr;

        void *res = reinterpret_cast<void *> (cPtr);
        SCIDB_ASSERT(res == static_cast<uint8_t*>(buf) + marshalledSize(nDims));
        return res;
    }

    /**
     * Unmarshall this from a buffer.
     * @param[in] buf a pointer to memory where to read from
     * @param[in] nDims the number of dimensions
     * @return ((uint8_t *)buf) + marshalledSize(nDims).
     */
    inline void const* unMarshall(void const* buf, size_t const nDims)
    {
        if (inputChunkPos.size())
        {
            inputChunkPos.clear();
        }

        Coordinate const* cPtr = reinterpret_cast<Coordinate const*>(buf);
        for(size_t i=0; i<nDims; i++)
        {
            inputChunkPos.push_back(*cPtr);
            ++cPtr;
        }

        size_t const* sPtr = reinterpret_cast<size_t const*>(cPtr);
        elementCount = *sPtr;
        ++sPtr;

        cPtr = reinterpret_cast<Coordinate const*>(sPtr);
        outputPos = *cPtr;
        ++cPtr;

        void const* res = reinterpret_cast<void const*> (cPtr);
        SCIDB_ASSERT(res == static_cast<uint8_t const*>(buf) + marshalledSize(nDims));
        return res;
    }
};

/**
 * A comparator for UnpackChunkAddresses.
 * We use the inputChunkPos coordinates for to keep addresses in sorted order.
 */
struct UnpackChunkAddressLess : public CoordinatesLess
{
    /**
     * Compare two addresses.
     * @return true if c1 is less than c2, false otherwise
     */
    bool operator()(const UnpackChunkAddress& c1, const UnpackChunkAddress& c2) const
    {
        return CoordinatesLess::operator()(c1.inputChunkPos, c2.inputChunkPos);
    }
};

/**
 * A marshallable set of UnpackChunkAddress-es whereby the starting position of
 * an output chunk can be looked up.
 */
class UnpackArrayInfo : public set <UnpackChunkAddress, UnpackChunkAddressLess>
{
private:
    /**
     * Number of dimensions in the input array.
     */
    size_t _nDims;

public:
    typedef set<UnpackChunkAddress, UnpackChunkAddressLess> Super;

    /**
     * Create an empty info.
     * @param[in] nDims the number of dimensions in the input array
     */
    UnpackArrayInfo(size_t const nDims):
        Super(),
        _nDims(nDims)
    {}

    /**
     * Given the position of the chunk in the input array - determine the position
     * of the starting element in this chunk in the output array. Throws if there is no
     * info for this chunk in this.
     * @param inputChunkPos the coordinates of the input chunk
     * @return the position of the first element of this chunk in the output array
     */
    Coordinate getOutputPos (Coordinates const& inputChunkPos) const
    {
        SCIDB_ASSERT(inputChunkPos.size() == _nDims);
        UnpackChunkAddress addr;
        addr.inputChunkPos = inputChunkPos;
        Super::const_iterator iter = Super::lower_bound(addr);
        if (iter == end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Can't find coordinates "<<CoordsToStr(inputChunkPos)<<" in set";
        }

        Coordinates const& iterPos = iter->inputChunkPos;
        for (size_t i=0; i<_nDims; i++)
        {
            if(iterPos[i] != inputChunkPos[i])
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Can't find coordinates "<<CoordsToStr(inputChunkPos)<<" in set";
            }
        }
        return iter->outputPos;
    }

    /**
     * Compute the marshalled size of the entire structure.
     * @return the number of bytes needed to marshall this
     */
    size_t getBinarySize() const
    {
        return UnpackChunkAddress::marshalledSize(_nDims) * Super::size() + sizeof(size_t);
    }

    /**
     * Write all the data into a preallocated buffer.
     * @param[in|out] buf a pointer to allocated memory; must be at least getBinarySize() bytes
     */
    void marshall(void* buf) const
    {
        size_t *sPtr = reinterpret_cast<size_t *> (buf);
        *sPtr = Super::size();
        ++sPtr;
        void *res = reinterpret_cast<char *>(sPtr);
        for(Super::iterator i = Super::begin(); i!=Super::end(); ++i)
        {
            res = i->marshall(res, _nDims);
        }
        SCIDB_ASSERT(res == static_cast<uint8_t*>(buf) + getBinarySize());
    }

    /**
     * Read marshalled data from the buffer and add it to this.
     * @param[in] buf a pointer to memory that contains a marshalled UnpackArrayInfo
     */
    void unMarshall(void const* buf)
    {
        size_t const* sPtr = reinterpret_cast<size_t const*>(buf);
        size_t numEntries = *sPtr;
        ++sPtr;
        void const* res = reinterpret_cast<void const *>(sPtr);
        for(size_t k =0; k<numEntries; k++)
        {
            UnpackChunkAddress addr;
            res = addr.unMarshall(res, _nDims);
            Super::iterator i = Super::find(addr);
            if( i == Super::end())
            {
                Super::insert(addr);
            }
            else
            {
                //Don't call me with partially filled chunks, buddy :)
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Unpack chunk info encountered the same chunk multiple times";
            }
        }
    }
};

/**
 * Print an UnpackArrayInfo into a text stream. Used for logging.
 * @param stream the output stream
 * @param info the data to print
 * @return the stream with the output added
 */
std::ostream& operator<<(std::ostream& stream, const UnpackArrayInfo& info)
{
    for (UnpackArrayInfo::iterator i = info.begin(); i!=info.end(); ++i)
    {
        stream<<CoordsToStr(i->inputChunkPos)<<","<<i->elementCount<<","<<i->outputPos<<" ";
    }
    return stream;
}

/**
 * Helper: given a vector of iterator pointers, check if they are set;  call flush() on numIterators first elements, then clear them.
 * @param[in] iterators the iterators to flush
 * @param[in] numIterators number of elements to flush
 */
template <class T>
inline void resetIterators(vector<shared_ptr<T> > & iterators, size_t const numIterators)
{
    for (size_t i=0; i<numIterators && iterators[i]; i++)
    {
        iterators[i]->flush();
        iterators[i].reset();
    }
}

/**
 * Helper: given a vector of iterator pointers, call operator++ on each.
 * @param[in] iterators the iterators to increment
 * @param[in] numIterators number of elements to increment
 */
template <class T>
inline void incrementIterators(vector<shared_ptr<T> > & iterators, size_t const numIterators)
{
    for (size_t i=0; i<numIterators; i++)
    {
        ++(*iterators[i]);
    }
}

/**
 * The Unpack Physical Operator.
 */
class PhysicalUnpack: public PhysicalOperator
{
private:
    Coordinate _outputChunkSize;

public:
    /**
     * Create the operator.
     * @param logicalName name of the logical op
     * @param physicalName name of the physical op
     * @param parameters all the parameters the op was called with
     * @param schema the result of LogicalUnpack::inferSchema
     */
    PhysicalUnpack(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _outputChunkSize(schema.getDimensions()[0].getChunkInterval())
    {}
    
    /**
     * @see PhysicalOperator::changesDistribution
     * @return true
     */
    virtual bool changesDistribution(std::vector<ArrayDesc> const& inputSchemas) const
    {
        return true;
    }

    /**
     * Determine whether the operator outputs full chunks.
     * @param inputSchemas the shapes of all the input arrays; one in our case
     * @return true if input is not emptyable and input chunk size matches output;
     *         false otherwise
     */
    virtual bool outputFullChunks(std::vector<ArrayDesc> const& inputSchemas) const
    {
        ArrayDesc const& inputSchema = inputSchemas[0];
        if (inputSchema.getEmptyBitmapAttribute())
        {   //input is emptyable - all bets are off
            return false;
        }
        Coordinate inputChunkSize = 1;
        for (size_t i=0, j=inputSchema.getDimensions().size(); i<j; i++)
        {
            inputChunkSize *= inputSchema.getDimensions()[i].getChunkInterval();
        }
        if (inputChunkSize == _outputChunkSize)
        {
            return true;
        }
        return false;
    }

    /**
     * Determine whether the operator outputs full chunks.
     * @return true if input is not emptyable and input chunk size matches output;
     *         false otherwise
     */
    virtual ArrayDistribution getOutputDistribution(std::vector<ArrayDistribution> const&, std::vector<ArrayDesc> const&) const
    {
        return ArrayDistribution(psUndefined);
    }

    /**
     * Compute the boundaries of the output array.
     * @return the input boundaries reshaped around a single dimension. Often an over-estimate.
     */
    virtual PhysicalBoundaries getOutputBoundaries(std::vector<PhysicalBoundaries> const& inputBoundaries, std::vector< ArrayDesc> const& inputSchemas) const
    {
        return inputBoundaries[0].reshape(inputSchemas[0].getDimensions(), _schema.getDimensions());
    }

    /**
     * Perform a single pass over some attribute of the inputArray and populate info with data about
     * the array.
     * @param[in] inputArray the array to iterate over
     * @param[out] info the structure to populate
     */
    void collectChunkInfo(shared_ptr<Array> const& inputArray, UnpackArrayInfo& info)
    {
        ArrayDesc const& desc = inputArray->getArrayDesc();
        AttributeID victimAttribute = 0;
        if( desc.getEmptyBitmapAttribute() )
        {
            victimAttribute = desc.getEmptyBitmapAttribute()->getId();
        }
        else
        {
            size_t minAttrSize = static_cast<size_t>(-1);
            for(AttributeID i = 0, j = desc.getAttributes().size(); i<j; i++)
            {
                AttributeDesc const& attr = desc.getAttributes()[i];
                if(attr.getSize() > 0 && attr.getSize() < minAttrSize)
                {
                    minAttrSize = attr.getSize();
                    victimAttribute = i;
                }
            }
        }
        shared_ptr<ConstArrayIterator> iter = inputArray->getConstIterator(victimAttribute);
        while (!iter->end())
        {
            UnpackChunkAddress addr;
            addr.inputChunkPos = iter->getPosition();
            ConstChunk const& chunk = iter->getChunk();
            addr.elementCount = chunk.count();
            addr.outputPos = 0;
            info.insert(addr);
            ++(*iter);
        }
    }

    /**
     * Send info to the coordinator; merge all sent data at coordinator and send
     * data back to all instances; rebuild info with data from all instances.
     * @param[in|out] info the structure to populate
     * @param[in] query the query context
     */
    void exchangeChunkInfo(UnpackArrayInfo& info, shared_ptr<Query>& query)
    {
        const size_t nInstances = query->getInstancesCount();
        if (! query->isCoordinator())
        {
            shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, info.getBinarySize()));
            info.marshall(buf->getData());
            info.clear();
            BufSend(query->getCoordinatorID(), buf, query);
            buf = BufReceive(query->getCoordinatorID(), query);
            info.unMarshall(buf->getData());
        }
        else
        {
            InstanceID myId = query->getInstanceID();
            for(InstanceID i=0; i<nInstances; i++)
            {
                if(i != myId)
                {
                    shared_ptr<SharedBuffer> buf = BufReceive(i,query);
                    info.unMarshall(buf->getData());
                }
            }
            shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, info.getBinarySize()));
            info.marshall(buf->getData());
            for(InstanceID i=0; i<nInstances; i++)
            {
                if(i != myId)
                {
                    BufSend(i, buf, query);
                }
            }
        }
    }

    /**
     * Build a UnpackArrayInfo from the local array, then exchange data with other nodes, then compute the starting
     * positions for each of the chunks.
     * @param[in] inputArray the array to scan
     * @param[in] query the query context
     * @param[out] info the data to collect
     */
    void computeGlobalChunkInfo(boost::shared_ptr<Array> const& inputArray, shared_ptr<Query>& query, UnpackArrayInfo& info)
    {
        collectChunkInfo(inputArray, info);
        exchangeChunkInfo(info, query);
        Coordinate startingPosition = 0;
        for(UnpackArrayInfo::iterator i = info.begin(); i != info.end(); ++i)
        {
            i->outputPos = startingPosition;
            startingPosition += i->elementCount;
        }
    }

    /**
     * Given an input array and an UnpackArrayInfo - create an outputArray by opening each chunk of the input, looking up
     * the corresponding position for the data in the output array and appending it.
     * @param[in] inputArray the array to take data from
     * @param[in] chunkInfo the information about where to place which chunks
     * @param[in] query the query context
     * @return the output MemArray with partially filled chunks wherein all the data elements are at the right place
     */
    shared_ptr<Array> fillOutputArray(shared_ptr<Array> const& inputArray, UnpackArrayInfo const& chunkInfo, shared_ptr<Query> &query)
    {
        shared_ptr<Array> result = make_shared<MemArray>(_schema,query);
        ArrayDesc const& inputSchema = inputArray->getArrayDesc();
        size_t nSrcDims = inputSchema.getDimensions().size();
        size_t nSrcAttrs = inputSchema.getAttributes(true).size();
        size_t startOfAttributes = inputSchema.getDimensions().size(); //remember that the first attributes of dst are dimensions from src
        size_t nDstAttrs = _schema.getAttributes(true).size();
        Coordinates outputChunkPos(0);
        Coordinates outputCellPos(1,0);
        vector<shared_ptr<ConstArrayIterator> > saiters (nSrcAttrs); //source array and chunk iters
        vector<shared_ptr<ConstChunkIterator> > sciters (nSrcAttrs);
        vector<shared_ptr<ArrayIterator> > daiters (nDstAttrs);      //destination array and chunk iters
        vector<shared_ptr<ChunkIterator> > dciters (nDstAttrs);
        for(AttributeID i =0; i<nSrcAttrs; i++)
        {
            saiters[i] = inputArray->getConstIterator(i);
        }
        for(AttributeID i =0; i<nDstAttrs; i++)
        {
            daiters[i] = result->getIterator(i);
        }

        BOOST_SCOPE_EXIT((&dciters)(&nDstAttrs)) {
            resetIterators(dciters, nDstAttrs);
        } BOOST_SCOPE_EXIT_END

        Value buf;
        while ( !saiters[0]->end() )
        {
            Coordinates const& chunkPos = saiters[0]->getPosition();
            outputCellPos[0] = chunkInfo.getOutputPos(chunkPos);
            for (AttributeID i = 0; i < nSrcAttrs; i++)
            {
                sciters[i] = saiters[i]->getChunk().getConstIterator();
            }
            while(!sciters[0]->end())
            {
                SCIDB_ASSERT(outputChunkPos.size() == 0 || outputCellPos[0] >= outputChunkPos[0] ); //can't go backwards!
                if(outputChunkPos.size() == 0 || outputCellPos[0] > (outputChunkPos[0] + _outputChunkSize) - 1)
                {
                    resetIterators(dciters, nDstAttrs);
                    outputChunkPos = outputCellPos;
                    _schema.getChunkPositionFor(outputChunkPos);
                    for (AttributeID i=0; i<nDstAttrs; i++)
                    {
                        Chunk& outChunk = daiters[i]->newChunk(outputChunkPos);
                        dciters[i] = outChunk.getIterator(query, i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : //populate empty tag from attr 0 implicitly
                                                                          ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
                    }
                }
                Coordinates const& inputCellPos = sciters[0]->getPosition();
                for(size_t i=0; i<nSrcDims; i++)
                {
                    dciters[i]->setPosition(outputCellPos);
                    buf.setInt64(inputCellPos[i]);
                    dciters[i]->writeItem(buf);
                }
                for(size_t i=0; i<nSrcAttrs; i++)
                {
                    dciters[i+startOfAttributes]->setPosition(outputCellPos);
                    dciters[i+startOfAttributes]->writeItem(sciters[i]->getItem());
                }
                outputCellPos[0]++;
                incrementIterators(sciters, nSrcAttrs);
            }
            incrementIterators(saiters, nSrcAttrs);
        }
        resetIterators(dciters, nDstAttrs);
        return result;
    }

    /**
     * Given the input array, first build an UnpackArrayInfo of how many elements each chunk has, then
     * redistribute the info to the coordinator, merge it, and use it to compute a place in the output array for
     * each chunk in the input; construct a MemArray with partially filled chunks where each element is in the proper
     * dense position. The operator will complete when the optimizer inserts redistribute after the operator and
     * merges the partially-filled chunks together.
     * @param[in] inputArrays only care about the first
     * @param[in] query the query context
     * @return the MemArray with partially filled chunks wherein each element is in the correct place
     */
    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        boost::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
        Dimensions const& dims = inputArray->getArrayDesc().getDimensions();

        UnpackArrayInfo info(dims.size());
        computeGlobalChunkInfo(inputArray, query, info);
        LOG4CXX_TRACE(logger, "Computed global chunk info "<<info);
        return fillOutputArray(inputArray, info, query);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalUnpack, "unpack", "physicalUnpack")

}  // namespace scidb
