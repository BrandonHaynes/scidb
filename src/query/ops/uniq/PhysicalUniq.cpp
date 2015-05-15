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

#include <query/Operator.h>
#include <util/Network.h>
#include <util/arena/Map.h>

#include "UniqSettings.h"

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.uniq"));

/**
 * @brief: The implementation of the uniq() operator.
 *
 * @par Algorithm:
 * <br> We perform one pass over the input array and, for each chunk, we record:
 * <br> - the chunk position
 * <br> - the first value in the chunk
 * <br> - the last value in the chunk
 * <br> - the number of unique elements in the chunk
 * <br>
 * <br> We then send this data to a single instance, which passes over the collected structures and, for each chunk in
 * the system, computes:
 * <br> - whether or not the first value in the chunk should be written to the output
 * <br> - the position in the output array where the first (or second) unique value in the chunk should be written to
 * <br>
 * <br> For example, suppose we have three instances with three chunks with the following data:
 * <br> Instance 0: Chunk {0}  -> a,a,a,b,b
 * <br> Instance 1: Chunk {5}  -> b,c,c,d,d
 * <br> Instance 2: Chunk {10} -> e,f,g,h,i
 * <br>
 * <br> In the first pass we compute the following information (InputArrayInfo):
 * <br> Chunk {0}  -> first=a, last=b, num_unique_values=2
 * <br> Chunk {5}  -> first=b, last=d, num_unique_values=3
 * <br> Chunk {10} -> first=e, last=i, num_unique_values=5
 * <br>
 * <br> We then send that information to a single instance which uses it to make the following map (OutputArrayInfo):
 * <br> Chunk {0},  outputCoordinate=0, writeFirst=true
 * <br> Chunk {5},  outputCoordinate=2, writeFirst=false
 * <br> Chunk {10}, outputCoordinate=4, writeFirst=true
 * <br>
 * <br> We send this map to all instances which use it to write the output array as follows:
 * <br> Instance 0: Chunk{0} -> a,b, , ,
 * <br> Instance 1: Chunk{0} ->  , ,c,d,
 * <br> Instance 2: Chunk{0} ->  , , , ,e
 * <br>             Chunk{5} -> f,g,h,i,
 * <br>
 * <br> We advertise to the SciDB optimizer that we are outputting partially-filled chunks. SciDB then does the job
 * of merging the data back together, outside of the operator. The end result looks like this:
 * <br> Instance 0: Chunk{0} -> a,b,c,d,e
 * <br> Instance 1: Chunk{5} -> f,g,h,i,
 * <br> Instance 2:
 *
 * @par A word about SciDB data distribution:
 * <br>
 * <br>
 * The default distribution scheme that SciDB uses is called "psHashPartitioned". In reality, it is a hash of the chunk
 * coordinates, modulo the number of instances. In the one-dimensional case, if data starts at 1 with a chunk size
 * of 10 on 3 instances, then chunk 1 goes to instance 0,  chunk 11 to instance 1, chunk 21 to instance 2, chunk 31 to
 * instance 0, and on...
 * <br>
 * <br>
 * In the two-plus dimensional case, the hash is not so easy to describe. For the exact definition, read
 * getInstanceForChunk() in Operator.cpp.
 * <br>
 * <br>
 * All data is currently stored with this distribution. But operators emit data in different distributions quite often.
 * For example, ops like cross, cross_join and some linear algebra routines will output data in a completely different
 * distribution. Worse, ops like slice, subarray, repart may emit "partially filled" or "ragged" chunks - just like
 * we do in the algorithm example above.
 * <br>
 * <br>
 * Data whose distribution is so "violated" must be redistributed before it is stored or processed by other ops that
 * need a particular distribution. The function redistribute() is available and is sometimes called directly by the
 * operator (see PhysicalIndexLookup.cpp for example). Other times, the operator simply tells the SciDB optimizer that
 * it may output data in an incorrect distribution. The optimizer then determines when and how to redistribute the data.
 * That approach is more advanatageous, as the optimizer is liable to get smarter about delaying or waiving the
 * call to redistribute(). For this purpose, the functions
 * <br> getOutputDistribution(),
 * <br> changedDistribution() and
 * <br> outputFullChunks()
 * are provided. See their usage in the operator code below.
 *
 * @author apoliakov@paradigm4.com
 */
class PhysicalUniq : public PhysicalOperator
{
private:

    /**
     * An assert like exception: throw and cancel the query if cond is false.
     * The benefit of this approach is that it does not bring the system down and, unlike asserts, is present in a
     * Release build.
     */
    static void EXCEPTION_ASSERT(bool cond)
    {
        if (! cond)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
    }

    /**
     * Information about a single chunk in the input array.
     */
    struct InputChunkInfo
    {
        size_t numUniqueElements;
        Value startingValue;
        Value endingValue;

        InputChunkInfo():
            numUniqueElements(0)
            /* note: the default Value constructor sets it to NULL */
        {}

        /**
         * Marshalling routines. Note that Values can be variable-sized.
         * @return the marshalled size of this structure
         */
        size_t getMarshalledSize() const
        {
            size_t ret = sizeof(size_t);
            if(numUniqueElements>=1)
            {
                ret += 2 * sizeof(size_t);
                ret += startingValue.size();
                ret += endingValue.size();
            }
            return ret;
        }

        /**
         * Marshall this onto a buffer.
         * @param ptr the buffer to write to; must point to at least getMarshalledSize() bytes of allocated memory.
         * @return ptr advanced forward by getMarshalledSize() bytes.
         */
        void* marshall(void* ptr) const
        {
            size_t* sptr = reinterpret_cast<size_t*>(ptr);
            *sptr = numUniqueElements;
            ++sptr;
            if(numUniqueElements>=1)
            {
                EXCEPTION_ASSERT(startingValue.isNull() == false && endingValue.isNull() == false);
                *sptr = startingValue.size();
                ++sptr;
                uint8_t* bptr = reinterpret_cast<uint8_t*>(sptr);
                memcpy(bptr, startingValue.data(), startingValue.size());
                bptr += startingValue.size();
                sptr = reinterpret_cast<size_t*>(bptr);
                *sptr = endingValue.size();
                ++sptr;
                bptr = reinterpret_cast<uint8_t*>(sptr);
                memcpy(bptr, endingValue.data(), endingValue.size());
                bptr += endingValue.size();
                sptr = reinterpret_cast<size_t*>(bptr);
            }
            return sptr;
        }

        /**
         * Populate this with data from a buffer.
         * @param ptr the buffer to read.
         * @return ptr advanced by the size of the data contained.
         */
        void* unmarshall(void* ptr)
        {
            size_t* sptr = reinterpret_cast<size_t*>(ptr);
            numUniqueElements = *sptr;
            ++sptr;
            if(numUniqueElements>=1)
            {
                size_t startingValueSize = *sptr;
                uint8_t* bptr = reinterpret_cast <uint8_t*>(sptr+1);
                startingValue.setData(bptr, startingValueSize);
                bptr += startingValueSize;
                sptr = reinterpret_cast<size_t*> (bptr);
                size_t endingValueSize = *sptr;
                bptr = reinterpret_cast <uint8_t*>(sptr+1);
                endingValue.setData(bptr, endingValueSize);
                bptr += endingValueSize;
                sptr = reinterpret_cast<size_t*> (bptr);
            }
            return sptr;
        }
    };

    /**
     * Information about where to write data in the output.
     */
    struct OutputChunkInfo
    {
        Coordinate startingPosition;
        bool writeFirstValue;

        OutputChunkInfo():
            startingPosition(-1),
            writeFirstValue(true)
        {}

        /**
         * @return the marshalled size of this
         */
        size_t getMarshalledSize() const
        {
            return sizeof(Coordinate) + sizeof(bool);
        }

        /**
         * Marshall this onto a buffer.
         * @param ptr the buffer to write to; must point to at least getMarshalledSize() bytes of allocated memory.
         * @return ptr advanced forward by getMarshalledSize() bytes.
         */
        void* marshall(void* ptr) const
        {
            Coordinate* cptr = reinterpret_cast<Coordinate*>(ptr);
            *cptr = startingPosition;
            ++cptr;
            bool* bptr = reinterpret_cast<bool*>(cptr);
            *bptr = writeFirstValue;
            ++bptr;
            return bptr;
        }

        /**
         * Populate this with data from a buffer.
         * @param ptr the buffer to read.
         * @return ptr advanced by the size of the data contained.
         */
        void* unmarshall(void* ptr)
        {
            Coordinate* cptr = reinterpret_cast<Coordinate*>(ptr);
            startingPosition = *cptr;
            ++cptr;
            bool* bptr = reinterpret_cast<bool*>(cptr);
            writeFirstValue = *bptr;
            ++bptr;
            return bptr;
        }
    };

    /**
     * A generic map of marshallable Elements ordered by Coordinate.
     * If this works out well, we might want to promote it to a publicly available object. We shall see...
     * There are bigger fish to fry at the moment.
     */
    template<typename Element>
    class MarshallableMap : private arena::managed::map<Coordinate, Element>
    {
    private:
        typedef arena::managed::map<Coordinate, Element> super;
        size_t _marshalledSize; //maintained as data is added

    public:
        typedef typename arena::managed::map<Coordinate, Element>::const_iterator const_iterator;

        MarshallableMap():
            _marshalledSize(sizeof(size_t))
        {}

        virtual ~MarshallableMap()
        {}

        virtual void addElement(Coordinate const position, Element const& input)
        {
            _marshalledSize += (input.getMarshalledSize() + sizeof(Coordinate));
            this->insert( pair<Coordinate,Element>(position, input) );
        }

        /**
         * Marshall this into a new SharedBuffer.
         * @return a buffer containing all of the data from this.
         */
        virtual shared_ptr<SharedBuffer> marshall() const
        {
            shared_ptr <SharedBuffer> result ( make_shared<MemoryBuffer>( static_cast<void*>(NULL), _marshalledSize));
            size_t* sptr = static_cast<size_t*> (result->getData());
            *sptr = super::size();
            ++sptr;
            for(const_iterator iter = super::begin(), end = super::end(); iter!= end; ++iter)
            {
                Coordinate* cptr = reinterpret_cast<Coordinate*> (sptr);
                *cptr = iter->first;
                ++cptr;
                sptr = reinterpret_cast<size_t*>(iter->second.marshall(cptr));
            }
            return result;
        }

        /**
         * Add data from a marshalled buffer to this.
         * @param buf a buffer obtained by calling marshall() on another object of this type.
         */
        virtual void unMarshall(shared_ptr<SharedBuffer>& buf)
        {
            size_t* sptr = static_cast<size_t*> (buf->getData());
            size_t numEntries = *sptr;
            ++sptr;
            for ( ; numEntries > 0; --numEntries)
            {
                Coordinate* cptr = reinterpret_cast<Coordinate*> (sptr);
                Coordinate coord = *cptr;
                ++cptr;
                Element elt;
                sptr = reinterpret_cast<size_t*>(elt.unmarshall(cptr));
                EXCEPTION_ASSERT(super::find(coord)==super::end());
                addElement(coord, elt);
            }
        }

        /**
         * @return constant iterator the beginning
         */
        const_iterator begin() const
        {
            return super::begin();
        }

        /**
         * @return constant iterator the end
         */
        const_iterator end() const
        {
            return super::end();
        }

        /**
         * @return constant iterator to the element at position, or end
         */
        const_iterator find( Coordinate const position) const
        {
            return super::find(position);
        }
    };

    class InputArrayInfo : public MarshallableMap <InputChunkInfo>
    {
    public:
        /**
         * Dump this to log for debug purposes
         * @param dataType the type of the data in the Values
         */
        void dumpToLog(TypeId const& dataType) const
        {
            if (!logger->isTraceEnabled())
            {
                return;
            }
            LOG4CXX_TRACE(logger, "InputArrayInfo dump:");
            /* Of note: here we ask SciDB to find a FunctionPointer to convert Values of this dataType to strings
             * Such a converter may or may not exist. If it doesn't, this call to findConverter will return NULL.
             */
            FunctionLibrary *flib = FunctionLibrary::getInstance();
            FunctionPointer converter = flib->findConverter(dataType, TID_STRING, false, false, NULL);
            for(const_iterator iter = begin(), fin = end(); iter!= fin; ++iter)
            {
                Coordinate coord = iter->first;
                InputChunkInfo const& chunkInfo = iter->second;
                if(chunkInfo.numUniqueElements == 0)
                {
                    LOG4CXX_TRACE(logger, ">>{"<<coord<<"} -> values 0");
                }
                else if (converter != NULL)
                {
                    Value result;
                    Value const* input = &chunkInfo.startingValue;
                    converter(&input, &result, NULL);
                    string startingValue = result.getString();
                    input = &chunkInfo.endingValue;
                    converter(&input, &result, NULL);
                    string endingValue = result.getString();
                    LOG4CXX_TRACE(logger, ">>{"<<coord<<"} -> values "<<chunkInfo.numUniqueElements
                                          <<" start "<<startingValue<<" end "<<endingValue);
                }
                else
                {
                    LOG4CXX_TRACE(logger, ">>{"<<coord<<"} -> values "<<chunkInfo.numUniqueElements
                                          <<" start [NON_CONVERTIBLE_TYPE] end [NON_CONVERTIBLE_TYPE]");
                }
            }
        }
    };

    /**
     * A marshall-able structure for output writing. Internals are very similar to InputArrayInfo.
     */
    class OutputArrayInfo : public MarshallableMap<OutputChunkInfo>
    {
    public:
        void dumpToLog() const
        {
            if (logger->isTraceEnabled() == false)
            {
                return;
            }
            LOG4CXX_TRACE(logger, "OutputArrayInfo dump:");
            for(const_iterator iter = begin(), fin = end(); iter != fin; ++iter)
            {
                Coordinate coord = iter->first;
                OutputChunkInfo const& chunkInfo = iter->second;
                LOG4CXX_TRACE(logger, ">>{"<<coord<<"} -> outputCoord {"<<chunkInfo.startingPosition<<"} writeFirst "
                              <<chunkInfo.writeFirstValue);
            }
        }
    };

    /**
     * A container used to write data to the output one-dimensional array, in row-major order.
     * The object is used to shorten the size of some of the functions involved and add some clarity.
     * Note: strictly speaking, we don't need to materialize the output. We could use a virtual array pattern as shown
     * in PhysicalIndexLookup. However, this is provided for some extra simplicity and as an example of writing array
     * data.
     * <br>
     * <br>
     * To conserve memory, the class requires that all data is written in order of increasing coordinates, and it keeps
     * only one chunk open for writing at any given time. Thus, previously finished chunks are swapped to disk when
     * writing a lot of data.
     */
    class OutputArraySequentialWriter : public boost::noncopyable
    {
    private:
        shared_ptr<Array> const _output;
        Coordinates _outputChunkPosition;
        Coordinates _outputCellPosition;
        shared_ptr<ArrayIterator> _outputArrayIterator;
        shared_ptr<ChunkIterator> _outputChunkIterator;

    public:
        OutputArraySequentialWriter(ArrayDesc const& schema, shared_ptr<Query>& query):
            _output(boost::make_shared<MemArray>(schema,query)),
            _outputChunkPosition(1, -1),
            _outputCellPosition(1, -1),
            _outputArrayIterator(_output->getIterator(0)) //the chunk iterator is NULL at the start
        {
            EXCEPTION_ASSERT(schema.getDimensions()[0].getStartMin() == 0 &&
                             schema.getDimensions().size() == 1 &&
                             schema.getAttributes(true).size() == 1);
        }

        /**
         * Write value into the output array at position.
         * @param position the location of the value; MUST be greater than the positions of all the previously written
         *                 values
         * @param value the data to write
         * @param query the query context
         */
        void writeValue(Coordinate const position, Value const& value, shared_ptr<Query>& query)
        {
            EXCEPTION_ASSERT(position > _outputCellPosition[0] && !value.isNull());
            _outputCellPosition[0] = position;
            Coordinates chunkPosition = _outputCellPosition;
            //Compute the chunk coordinate from the cell coordinate
            _output->getArrayDesc().getChunkPositionFor(chunkPosition);
            if (chunkPosition[0] != _outputChunkPosition[0])  //first chunk, or a new chunk
            {
                if (_outputChunkIterator)
                {
                    _outputChunkIterator->flush(); //flush the last chunk if any
                }
                _outputChunkPosition = chunkPosition;
                //open the new chunk
                _outputChunkIterator = _outputArrayIterator->newChunk(chunkPosition).getIterator(query,
                                                                                       ChunkIterator::SEQUENTIAL_WRITE);
            }
            //write
            _outputChunkIterator->setPosition(_outputCellPosition);
            _outputChunkIterator->writeItem(value);
        }

        /**
         * Flush the last chunk and return the resulting array object. After this, the class is invalidated.
         * @return the output array
         */
        shared_ptr<Array> finalize()
        {
            if(_outputChunkIterator)
            {
                _outputChunkIterator->flush();
                _outputChunkIterator.reset();
            }
            _outputArrayIterator.reset();
            return _output;
        }
    };


public:
    PhysicalUniq(string const& logicalName,
                 string const& physicalName,
                 Parameters const& parameters,
                 ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}


private:
    /**
     * Read the input array (whatever part of it we have on the local instance) and populate an inputArrayInfo struct.
     */
    void fillInputArrayInfo(shared_ptr<Array>& inputArray, InputArrayInfo& inputArrayInfo)
    {
        shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
        while(!inputArrayIter->end())
        {
            ConstChunk const& inputChunk = inputArrayIter->getChunk();
            Coordinate chunkCoord = inputArrayIter->getPosition()[0]; ///get the position of the chunk
            InputChunkInfo inputChunkInfo;
            shared_ptr<ConstChunkIterator> inputChunkIter = inputChunk.getConstIterator();
            while(!inputChunkIter->end())
            {
                Value& val = inputChunkIter->getItem();
                if(!val.isNull())
                {
                    if(inputChunkInfo.startingValue.isNull()) //first value
                    {
                        ++inputChunkInfo.numUniqueElements;
                        inputChunkInfo.startingValue = val;
                        inputChunkInfo.endingValue = val;
                    }
                    else if(inputChunkInfo.endingValue != val)
                    {
                        ++inputChunkInfo.numUniqueElements;
                        inputChunkInfo.endingValue = val;
                    }
                }
                ++(*inputChunkIter);
            }
            inputArrayInfo.addElement(chunkCoord, inputChunkInfo);
            ++(*inputArrayIter);
        }
    }

    /**
     * Iterate over an InputArrayInfo object and compute an OutputArrayInfo. Called on one instance with global data.
     * See the algorithm at the begininning of the file for details.
     */
    void computeOutputArrayInfo(InputArrayInfo const& inputArrayInfo, OutputArrayInfo& outputArrayInfo)
    {
        Value lastVal;
        Coordinate nextPosition = 0;
        for (InputArrayInfo::const_iterator iter = inputArrayInfo.begin(), end = inputArrayInfo.end();
             iter != end; ++ iter)
        {
            Coordinate inputChunkPosition = iter->first;
            OutputChunkInfo outputInfo;
            InputChunkInfo const& inputInfo = iter->second;
            if (inputInfo.numUniqueElements == 0 ||  //could happen if the chunk has all null
                (lastVal == inputInfo.startingValue && inputInfo.numUniqueElements == 1))
            {
                outputInfo.startingPosition = -1; //skip this chunk completely!
            }
            else
            {
                outputInfo.startingPosition = nextPosition;
                if (lastVal == inputInfo.startingValue)
                {
                    outputInfo.writeFirstValue = false;
                    nextPosition += (inputInfo.numUniqueElements - 1);
                }
                else
                {
                    nextPosition += inputInfo.numUniqueElements;
                }
                lastVal = inputInfo.endingValue;
            }
            outputArrayInfo.addElement(inputChunkPosition, outputInfo);
        }
    }

    /**
     * Given an inputArrayInfo from this instance, marshall it, send it to to all the other instances, then receive
     * info from all other instances, merge and compute the outputArrayInfo.
     * @param inputArrayInfo the input array info computed on the local instance
     * @param[out] outputArrayInfo the output placeholder; after the routine returns, it contains the valid info on all
     *             instances
     * @param query the query context
     */
    void exchangeArrayInfo(InputArrayInfo& inputArrayInfo, OutputArrayInfo& outputArrayInfo, shared_ptr<Query>& query)
    {
        shared_ptr<SharedBuffer> buf = inputArrayInfo.marshall();
        InstanceID myInstanceId = query->getInstanceID();
        for (InstanceID i = 0; i< query->getInstancesCount(); ++i)
        {
            if (i == myInstanceId)
            {
                continue;
            }
            BufSend(i, buf, query);   //send to instance 0
        }
        for (InstanceID i =0; i< query->getInstancesCount(); ++i)
        {
            if (i == myInstanceId)
            {
                continue;
            }
            buf = BufReceive(i,query);
            inputArrayInfo.unMarshall(buf);
        }
        computeOutputArrayInfo(inputArrayInfo, outputArrayInfo);
    }

    /**
     * Given the input array and an OutputArrayInfo, populate and return the local portion of the output array.
     */
    shared_ptr<Array> writeOutputArray(shared_ptr<Array>& inputArray, OutputArrayInfo const& outputArrayInfo,
                                       shared_ptr<Query>& query)
    {
        OutputArraySequentialWriter outputWriter(_schema, query);
        for (shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
             !inputArrayIter->end();
             ++(*inputArrayIter))  //for each chunk in input
        {
            ConstChunk const& inputChunk = inputArrayIter->getChunk();
            Coordinate inputChunkPosition = inputArrayIter->getPosition()[0];
            OutputArrayInfo::const_iterator iter = outputArrayInfo.find(inputChunkPosition); //find entry for this chunk
            EXCEPTION_ASSERT(iter != outputArrayInfo.end());
            if (iter->second.startingPosition < 0)
            {   //we are told to skip the chunk
                continue;
            }
            Coordinate currentOutputPos = iter->second.startingPosition; //write data to output starting at this pos
            Value lastVal; //constructed as null
            for(shared_ptr<ConstChunkIterator> inputChunkIter = inputChunk.getConstIterator();
                !inputChunkIter->end();
                ++(*inputChunkIter)) //for each value in the chunk
            {
                Value& inputValue = inputChunkIter->getItem();
                if(lastVal != inputValue) //new unique value or first value
                {
                    if(iter->second.writeFirstValue || !lastVal.isNull())
                    {
                        outputWriter.writeValue(currentOutputPos, inputValue, query);
                        ++currentOutputPos;
                    }
                    lastVal = inputValue;
                }
            }
        }
        return outputWriter.finalize();
    }

public:
    /**
     * Tell the optimizer whether this operator will change the data distribution of its input array. Applies only
     * to operators that have input arrays and false by default. Called on the coordinator at planning time.
     * @param inputSchemas the shapes of the input arrays (in case it influences the decision).
     */
    virtual bool changesDistribution(vector<ArrayDesc> const& inputSchemas) const
    {
       return true;
    }

    /**
     * Get the distribution of the output array of this operator. Called on the coordinator at planning time.
     * @param inputDistributions the distributions of all the input arrays
     * @param inputSchemas the shapes of the input arrays.
     */
    virtual ArrayDistribution getOutputDistribution(vector<ArrayDistribution> const&,
                                                    vector<ArrayDesc> const&) const
    {
       /* Usually the answer is either "psHashPartitioned" or "psUndefined". Other distributions are a more advanced topic.
        * psUndefined is a catch-all.
        */
       return ArrayDistribution(psUndefined);
    }

    /**
     * Tell the optimizer whether this operator will output "full" or "partial" chunks. Partial chunks means that two
     * different instances may have a chunk with the same position, each instance having a portion of the data. Called
     * on the coordinator at planning time. By default the routine returns true - meaning output chunks are full.
     * @param inputSchemas the shapes of the input arrays.
     */
    virtual bool outputFullChunks(vector<ArrayDesc> const& inputSchemas) const
    {
        return false;
    }

    /**
     * If possible, tell the optimizer the bounding box of the output array. The bounding box is defined as a pair of
     * coordinates, one for the upper-left cell in the array and one for the bottom-right cell in the array. For
     * unbounded arrays, this allows the optimizer to estimate the size of the returned result. By default,
     * the optimizer assumes that every cell in the output schema is occupied (worst case)
     */
    virtual PhysicalBoundaries getOutputBoundaries(vector<PhysicalBoundaries> const & inputBoundaries,
                                                   vector< ArrayDesc> const & inputSchemas) const
    {
       /* In our case, we don't know how many values we will eliminate, so we return the boundaries of the input.
        * It tells the optimizer we cannot possibly increase the size of the input - better than nothing.
        * Over time, we may add more sophisticated statistics and hints like this.
        * XXX: Careful: If we allow input to start at position other than 0, we must alter this to shift the boundaries.
        */
       return inputBoundaries[0];
    }

    shared_ptr<Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        InputArrayInfo inputArrayInfo;
        fillInputArrayInfo(inputArrays[0], inputArrayInfo);
        OutputArrayInfo outputArrayInfo;
        inputArrayInfo.dumpToLog(inputArrays[0]->getArrayDesc().getAttributes()[0].getType());
        exchangeArrayInfo(inputArrayInfo, outputArrayInfo, query);
        outputArrayInfo.dumpToLog();
        return writeOutputArray(inputArrays[0], outputArrayInfo, query);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalUniq, "uniq", "PhysicalUniq")

} //namespace scidb
