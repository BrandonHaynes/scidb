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
 * @file Operator.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of basic operator methods.
 */

#include <boost/make_shared.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/unordered_set.hpp>
#include <log4cxx/logger.h>

#include <query/QueryPlanUtilites.h>
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <network/NetworkManager.h>
#include <network/BaseConnection.h>
#include <network/MessageUtils.h>
#include <system/SystemCatalog.h>
#include <array/DBArray.h>
#include <array/TransientCache.h>
#include <query/QueryProcessor.h>
#include <system/BlockCyclic.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <smgr/io/Storage.h>
#include <boost/functional/hash.hpp>
#include <util/Hashing.h>
#include <util/Timing.h>
#include <util/MultiConstIterators.h>

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.operator"));

template <typename T>
inline static T min( T const& lhs, T const& rhs)
{
    return lhs < rhs ? lhs : rhs;
}

template <typename T>
inline static T max( T const& lhs, T const& rhs)
{
    return lhs > rhs ? lhs : rhs;
}


#define stringify(name) #name

static const char *OperatorParamPlaceholderTypeNames[] =
{
        stringify(PLACEHOLDER_INPUT),
        stringify(PLACEHOLDER_ARRAY_NAME),
        stringify(PLACEHOLDER_ATTRIBUTE_NAME),
        stringify(PLACEHOLDER_DIMENSION_NAME),
        stringify(PLACEHOLDER_CONSTANT),
        stringify(PLACEHOLDER_EXPRESSION),
        stringify(PLACEHOLDER_VARIES),
        stringify(PLACEHOLDER_SCHEMA),
        stringify(PLACEHOLDER_AGGREGATE_CALL),
        stringify(PLACEHOLDER_END_OF_VARIES)
};

void OperatorParamPlaceholder::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    int typeIndex = 0, type = _placeholderType;
    while ((type >>= 1) != 0) {
        typeIndex += 1;
    }
    out << "[opParamPlaceholder] " << OperatorParamPlaceholderTypeNames[typeIndex];
    out << " requiredType " <<_requiredType.name();
    out <<" ischeme "<<_inputSchema<<"\n";
}

void OperatorParam::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out <<"[param] type "<<_paramType<<"\n";
}

void OperatorParamReference::toString(std::ostream &out, int /*indent*/) const
{
    out << "object "<<_objectName;
    out << " inputNo " <<_inputNo;
    out << " objectNo " <<_objectNo;
    out << " inputScheme "<<_inputScheme;
    out << "\n";
}

void OperatorParamArrayReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramArrayReference] ";
    OperatorParamReference::toString(out, indent);
}

void OperatorParamAttributeReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAttributeReference] ";
    OperatorParamReference::toString(out,indent);
}

void OperatorParamDimensionReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramDimensionReference] ";
    OperatorParamReference::toString(out,indent);
}

void OperatorParamLogicalExpression::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramLogicalExpression] type "<<_expectedType.name();
    out << " const " << _constant << "\n";
    _expression->toString(out, indent+1);
}

void OperatorParamPhysicalExpression::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramPhysicalExpression] const " << _constant;
    out << "\n";
    _expression->toString(out, indent+1);
}

void OperatorParamSchema::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out <<"[paramSchema] " << _schema <<"\n";
}

void OperatorParamAggregateCall::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAggregateCall] " << _aggregateName << "\n" ;

    out << prefix(' ', false);
    out <<"input: ";
    _inputAttribute->toString(out);

    if (_alias.size() )
    {
        out << prefix(' ');
        out << "alias " << _alias << "\n";
    }
}

void OperatorParamAsterisk::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAsterisk] *" << "\n";
}

void LogicalOperator::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lOperator] "<<_logicalName;
    out << " ddl "<< _properties.ddl << "\n";

    for (size_t i = 0; i < _parameters.size(); i++)
    {
        _parameters[i]->toString(out, indent+1);
    }

    for ( size_t i = 0; i < _paramPlaceholders.size(); i++)
    {
        _paramPlaceholders[i]->toString(out,indent+1);
    }

    out << prefix('>', false);
    out << "schema: " << _schema << "\n";
}

void PhysicalOperator::setQuery(const boost::shared_ptr<Query>& query)
{
    arena::Options options;                              // Arena ctor args

    options.name  (_physicalName.c_str());               // Use operator name
    options.parent(query->getArena());                   // Attach to query
    options.threading(true);                             // Assume threaded

    _query = query;                                      // Save the query
    _arena = arena::newArena(options);                   // Create new Arena
}

void PhysicalOperator::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "schema " <<_schema<<"\n";
}

void PhysicalOperator::dumpArrayToLog(shared_ptr<Array> const& input, log4cxx::LoggerPtr& logger)
{
    ArrayDesc const& schema = input->getArrayDesc();
    Attributes const& attrs = schema.getAttributes(true);
    size_t const nAttrs = attrs.size();
    vector<FunctionPointer> converters(nAttrs,NULL);
    FunctionLibrary *functionLib = FunctionLibrary::getInstance();
    vector<shared_ptr<ConstArrayIterator> > aiters (nAttrs);
    vector<shared_ptr<ConstChunkIterator> > citers (nAttrs);
    for (size_t i =0; i<nAttrs; ++i)
    {
        TypeId const& typeId = attrs[i].getType();
        converters[i] = functionLib->findConverter(typeId, TID_STRING, false, false, NULL);
        aiters[i] = input->getConstIterator(i);
    }
    while (!aiters[0]->end())
    {
        for(size_t i=0; i<nAttrs; ++i)
        {
            citers[i] = aiters[i]->getChunk().getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
        }
        while (!citers[0]->end())
        {
            Coordinates const& position = citers[0]->getPosition();
            ostringstream out;
            out<<CoordsToStr(position)<<" ";
            for(size_t i=0; i<nAttrs; ++i)
            {
                Value const& v = citers[i]->getItem();
                if (v.isNull())
                {
                    if(v.getMissingReason() == 0)
                    {
                        out<<"[null]";
                    }
                    else
                    {
                        out<<"[?"<<v.getMissingReason()<<"]";
                    }
                }
                else if (converters[i])
                {
                    Value const* input = &v;
                    Value result;
                    converters[i](&input, &result, NULL);
                    out<<result.getString();
                }
                else
                {
                    out<<"[nct]";
                }
                out<<",";
            }
            LOG4CXX_DEBUG(logger, out.str());
            for(size_t i=0; i<nAttrs; ++i)
            {
                ++(*citers[i]);
            }
        }
        for(size_t i=0; i<nAttrs; ++i)
        {
            ++(*aiters[i]);
        }
    }
}

shared_ptr<Array>
PhysicalOperator::ensureRandomAccess(shared_ptr<Array>& input,
                                     shared_ptr<Query> const& query)
{
    if (input->getSupportedAccess() == Array::RANDOM)
    {
        return input;
    }
    LOG4CXX_DEBUG(logger, "Query "<<query->getQueryID()<<
                  " materializing input "<<input->getArrayDesc());
    bool vertical = (input->getSupportedAccess() == Array::MULTI_PASS);
    shared_ptr<MemArray> memCopy(new MemArray(input, query, vertical));
    input.reset();

    return memCopy;
}

/*
 * PhysicalBoundaries methods
 */
PhysicalBoundaries::PhysicalBoundaries(Coordinates const& start, Coordinates const& end, double density):
    _startCoords(start), _endCoords(end), _density(density)
{
    if (_startCoords.size() != _endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);

    if (density < 0.0 || density > 1.0)
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);

    for (size_t i = 0; i< _startCoords.size(); i++)
    {
        if (_startCoords[i] < MIN_COORDINATE)
        {
            _startCoords[i] = MIN_COORDINATE;
        }
        else if (_startCoords[i] > MAX_COORDINATE)
        {
            _startCoords[i] = MAX_COORDINATE;
        }

        if (_endCoords[i] < MIN_COORDINATE)
        {
            _endCoords[i] = MIN_COORDINATE;
        }
        else if (_endCoords[i] > MAX_COORDINATE)
        {
            _endCoords[i] = MAX_COORDINATE;
        }
    }
}

PhysicalBoundaries PhysicalBoundaries::createFromFullSchema(ArrayDesc const& schema )
{
    Coordinates resultStart, resultEnd;

    for (size_t i =0; i<schema.getDimensions().size(); i++)
    {
        resultStart.push_back(schema.getDimensions()[i].getStartMin());
        resultEnd.push_back(schema.getDimensions()[i].getEndMax());
    }

    return PhysicalBoundaries(resultStart, resultEnd);
}

PhysicalBoundaries PhysicalBoundaries::createEmpty(size_t numDimensions)
{
    Coordinates resultStart, resultEnd;

    for (size_t i =0; i<numDimensions; i++)
    {
        resultStart.push_back(MAX_COORDINATE);
        resultEnd.push_back(MIN_COORDINATE);
    }

    return PhysicalBoundaries(resultStart, resultEnd);
}

bool PhysicalBoundaries::isEmpty() const
{
    if (_startCoords.size() == 0)
    {
        return true;
    }

    for (size_t i = 0; i<_startCoords.size(); i++)
    {
        if (_startCoords[i] > _endCoords[i])
        {
            return true;
        }
    }
    return false;
}

uint64_t PhysicalBoundaries::getCellNumber (Coordinates const& coords, Dimensions const& dims)
{
    if (dims.size() != coords.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);
    }
    uint64_t result = 0;
    for ( size_t i = 0, n = dims.size(); i < n; i++)
    {
        if (dims[i].getLength()==0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);
        }
        uint64_t t = result * dims[i].getLength();
        if (t / dims[i].getLength() != result) //overflow check multiplication
        {
            return INFINITE_LENGTH;
        }
        result = t;
        t = result + coords[i] - dims[i].getStartMin();
        if (t < result) //overflow check addition
        {
            return INFINITE_LENGTH;
        }
        result = t;
    }
    if (result >= INFINITE_LENGTH)
    {
        return INFINITE_LENGTH;
    }
    return result;
}

Coordinates PhysicalBoundaries::getCoordinates(uint64_t& cellNum, Dimensions const& dims, bool strictCheck)
{
    if(cellNum >= INFINITE_LENGTH)
    {
        return Coordinates(dims.size(), MAX_COORDINATE);
    }
    Coordinates result (dims.size(), 0);
    for (int i = dims.size(); --i >= 0;)
    {
        result[i] = dims[i].getStartMin() + (cellNum % dims[i].getLength());
        cellNum /= dims[i].getLength();
    }
    if (strictCheck && cellNum != 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);
    }
    return result;
}

Coordinates PhysicalBoundaries::reshapeCoordinates (Coordinates const& in,
                                                    Dimensions const& currentDims,
                                                    Dimensions const& newDims)
{
    uint64_t cellNum = getCellNumber(in, currentDims);
    if ( cellNum >= INFINITE_LENGTH )
    {
        return Coordinates(newDims.size(), MAX_COORDINATE);
    }
    return getCoordinates(cellNum, newDims);
}

uint64_t PhysicalBoundaries::getNumCells (Coordinates const& start, Coordinates const& end)
{
    if (PhysicalBoundaries(start,end).isEmpty())
    {
        return 0;
    }
    uint64_t result = 1;
    for ( size_t i = 0; i < end.size(); i++)
    {
        if (start[i] <= MIN_COORDINATE || end[i] >= MAX_COORDINATE)
        {
            return INFINITE_LENGTH;
        }
        else if(end[i] >= start[i])
        {
            uint64_t t = result * (end[i] - start[i] + 1);
            if ( t / (end[i] - start[i] + 1) != result) //overflow check multiplication
            {
                return INFINITE_LENGTH;
            }
            result = t;
        }
        else
        {
            result *= 0;
        }
    }
    if (result >= INFINITE_LENGTH)
    {
        return INFINITE_LENGTH;
    }
    return result;
}

uint64_t PhysicalBoundaries::getNumCells() const
{
    return getNumCells(_startCoords, _endCoords);
}

uint64_t PhysicalBoundaries::getCellsPerChunk (Dimensions const& dims)
{
    uint64_t cellsPerChunk = 1;
    for (size_t i = 0; i<dims.size(); i++)
    {
        //assume the dimensions that are passed in come from an array, therefore this is overflow-safe
        cellsPerChunk *= dims[i].getChunkInterval();
    }
    return cellsPerChunk;
}

uint64_t PhysicalBoundaries::getNumChunks(Dimensions const& dims) const
{
    if (_startCoords.size() != dims.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);
    }

    if (isEmpty())
    {
        return 0;
    }

    uint64_t result = 1;
    for (size_t i =0; i < _endCoords.size(); i++)
    {
        if (_startCoords[i]<=MIN_COORDINATE || _endCoords[i]>=MAX_COORDINATE)
        {
            return INFINITE_LENGTH;
        }
        DimensionDesc const& dim = dims[i];
        if (_startCoords[i] < dim.getStartMin() || _endCoords[i] > dim.getEndMax())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);
        }
        Coordinate arrayStart = dim.getStartMin();
        int64_t chunkInterval = dim.getChunkInterval();
        Coordinate physStart = _startCoords[i]; //TODO:OPTAPI (- overlap) ?
        Coordinate physEnd = _endCoords[i];
        int64_t numChunks = chunkInterval == 0 ? 0 :
                            ((physEnd - arrayStart + chunkInterval) / chunkInterval) - ((physStart - arrayStart) / chunkInterval);
        uint64_t t = result * numChunks;
        if ( numChunks && t / numChunks != result) //overflow check multiplication
        {
            return INFINITE_LENGTH;
        }
        result = t;
    }
    return result;
}

std::ostream& operator<< (std::ostream& stream, const PhysicalBoundaries& bounds)
{
    stream<<"start "<<bounds._startCoords<<" end "<<bounds._endCoords<<" density "<<bounds._density;
    return stream;
}

bool PhysicalBoundaries::isInsideBox (Coordinate const& in, size_t const& dimensionNum) const
{
    if (dimensionNum >= _startCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);

    Coordinate start = _startCoords[dimensionNum];
    Coordinate end = _endCoords[dimensionNum];

    return in >= start && in <= end;
}

PhysicalBoundaries PhysicalBoundaries::intersectWith (PhysicalBoundaries const& other) const
{
    if (_startCoords.size() != other._startCoords.size()
            || _startCoords.size() != other._endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_BOUNDARIES);

    if (isEmpty() || other.isEmpty())
    {
        return createEmpty(_startCoords.size());
    }

    Coordinates start;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        Coordinate myStart = _startCoords[i];
        Coordinate otherStart = other._startCoords[i];
        start.push_back( myStart < otherStart ? otherStart : myStart );
    }

    Coordinates end;
    for (size_t i =0; i<_endCoords.size(); i++)
    {
        Coordinate myEnd = _endCoords[i];
        Coordinate otherEnd = other._endCoords[i];
        end.push_back( myEnd > otherEnd ? otherEnd : myEnd);
    }

    double myCells = getNumCells();
    double otherCells = other.getNumCells();
    double intersectionCells = getNumCells(start,end);

    double resultDensity = 1.0;
    if (intersectionCells > 0)
    {
        double maxMyDensity = min( _density * myCells / intersectionCells, 1.0 );
        double maxOtherDensity = min ( other._density * otherCells / intersectionCells, 1.0);
        resultDensity = min (maxMyDensity, maxOtherDensity);
    }

    return PhysicalBoundaries(start,end, resultDensity);
}

PhysicalBoundaries PhysicalBoundaries::unionWith (PhysicalBoundaries const& other) const
{
    if (_startCoords.size() != other._startCoords.size()
            || _startCoords.size() != other._endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_BOUNDARIES);

    if (isEmpty())
    {
        return other;
    }

    else if (other.isEmpty())
    {
        return *this;
    }

    Coordinates start;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        Coordinate myStart = _startCoords[i];
        Coordinate otherStart = other._startCoords[i];
        start.push_back( myStart > otherStart ? otherStart : myStart );
    }

    Coordinates end;
    for (size_t i =0; i<_endCoords.size(); i++)
    {
        Coordinate myEnd = _endCoords[i];
        Coordinate otherEnd = other._endCoords[i];
        end.push_back( myEnd < otherEnd ? otherEnd : myEnd);
    }

    double myCells = getNumCells();
    double otherCells = getNumCells();
    double resultCells = getNumCells(start, end);
    double maxDensity = min ( (myCells * _density + otherCells * other._density ) / resultCells, 1.0);

    return PhysicalBoundaries(start,end, maxDensity);
}

PhysicalBoundaries PhysicalBoundaries::crossWith (PhysicalBoundaries const& other) const
{
    if (isEmpty() || other.isEmpty())
    {
        return createEmpty(_startCoords.size()+other._startCoords.size());
    }

    Coordinates start, end;
    for (size_t i=0; i<_startCoords.size(); i++)
    {
        start.push_back(_startCoords[i]);
        end.push_back(_endCoords[i]);
    }
    for (size_t i=0; i<other.getStartCoords().size(); i++)
    {
        start.push_back(other.getStartCoords()[i]);
        end.push_back(other.getEndCoords()[i]);
    }

    return PhysicalBoundaries(start,end, _density * other._density);
}

PhysicalBoundaries PhysicalBoundaries::reshape(Dimensions const& oldDims, Dimensions const& newDims) const
{
    if (isEmpty())
    {
        return createEmpty(newDims.size());
    }

    Coordinates start = reshapeCoordinates(_startCoords, oldDims, newDims);
    Coordinates end = reshapeCoordinates(_endCoords, oldDims, newDims);

    if (newDims.size() > oldDims.size())
    {
        bool dimensionFull = false;

        for (size_t i = 0; i < start.size(); i++)
        {
            if (dimensionFull)
            {
                if (newDims[i].getStartMin() <= MIN_COORDINATE || newDims[i].getEndMax() >= MAX_COORDINATE)
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_BOUNDARIES_FROM_INFINITE_ARRAY);

                start[i] = newDims[i].getStartMin();
                end[i] = newDims[i].getEndMax();
            }
            else if(end[i] > start[i])
            {
                dimensionFull = true;
            }
        }
    }

    double startingCells = getNumCells();
    double resultCells = getNumCells(start, end);

    return PhysicalBoundaries(start,end, _density * startingCells / resultCells);
}

shared_ptr<SharedBuffer> PhysicalBoundaries::serialize() const
{
    size_t totalSize = sizeof(size_t) + sizeof(double) + _startCoords.size()*sizeof(Coordinate) + _endCoords.size()*sizeof(Coordinate);
    MemoryBuffer* buf = new MemoryBuffer(NULL, totalSize);

    size_t* sizePtr = (size_t*)buf->getData();
    *sizePtr = _startCoords.size();
    sizePtr++;

    double* densityPtr = (double*) sizePtr;
    *densityPtr = _density;
    densityPtr++;

    Coordinate* coordPtr = (Coordinate*) densityPtr;

    for(size_t i = 0; i<_startCoords.size(); i++)
    {
        *coordPtr = _startCoords[i];
        coordPtr++;
    }

    for(size_t i = 0; i<_endCoords.size(); i++)
    {
        *coordPtr = _endCoords[i];
        coordPtr++;
    }

    assert((char*) coordPtr == (char*)buf->getData() + buf->getSize());
    return shared_ptr<SharedBuffer> (buf);
}

PhysicalBoundaries PhysicalBoundaries::deSerialize(shared_ptr<SharedBuffer> const& buf)
{
    size_t* numCoordsPtr = (size_t*) buf->getData();
    size_t numCoords = *numCoordsPtr;
    numCoordsPtr++;

    double* densityPtr = (double*) buf->getData();
    double density = *densityPtr;
    densityPtr++;

    Coordinates start, end;
    Coordinate* coordPtr = (Coordinate*) densityPtr;

    for(size_t i =0; i<numCoords; i++)
    {
        start.push_back(*coordPtr);
        coordPtr++;
    }

    for(size_t i =0; i<numCoords; i++)
    {
        end.push_back(*coordPtr);
        coordPtr++;
    }

    return PhysicalBoundaries(start,end,density);
}


uint32_t PhysicalBoundaries::getCellSizeBytes(const Attributes& attrs )
{
    uint32_t totalBitSize = 0;
    Config* cfg = Config::getInstance();

    for (size_t i = 0; i < attrs.size(); i++)
    {
        const AttributeDesc attr = attrs[i];
        Type cellType = TypeLibrary::getType(attr.getType());
        uint32_t bitSize = cellType.bitSize();

        if (bitSize == 0)
        {
            bitSize =  cfg->getOption<int>(CONFIG_STRING_SIZE_ESTIMATION) * 8;
        }
        if (attr.isNullable())
        {
            bitSize += 1;
        }
        totalBitSize += bitSize;
    }
    return (totalBitSize + 7)/8;
}

double PhysicalBoundaries::getSizeEstimateBytes(const ArrayDesc& schema) const
{
    uint64_t numCells = getNumCells();
    uint64_t numChunks = getNumChunks(schema.getDimensions());
    size_t numDimensions = schema.getDimensions().size();
    size_t numAttributes = schema.getAttributes().size();

    uint32_t cellSize = getCellSizeBytes(schema.getAttributes());

    //we assume that every cell is part of sparse chunk
    cellSize += numAttributes * (numDimensions  * sizeof(Coordinate) + sizeof(int));
    double size = numCells * 1.0 * cellSize ;

    //Assume all chunks are sparse and add header
    size += numChunks *
            numAttributes;

    return size * _density;
}


/*
 * ArrayDistribution methods
 */
bool operator== (ArrayDistribution const& lhs, ArrayDistribution const& rhs)
{
    return lhs._partitioningSchema == rhs._partitioningSchema &&
           (lhs._partitioningSchema != psLocalInstance || lhs._instanceId == rhs._instanceId) &&
           ((!lhs.hasMapper() && !rhs.hasMapper()) || ( lhs.hasMapper() && rhs.hasMapper() && *lhs._distMapper.get() == *rhs._distMapper.get()));
}

std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& dist)
{
    switch (dist._partitioningSchema)
    {
        case psReplication:     stream<<"repl";
                                break;
        case psHashPartitioned: stream<<"hash";
                                break;
        case psLocalInstance:   stream<<"loca";
                                break;
        case psByRow:           stream<<"byro";
                                break;
        case psByCol:           stream<<"byco";
                                break;
        case psUndefined:       stream<<"undefined";
                                break;
        case psGroupby:         stream<<"groupby";
                                break;
        case psScaLAPACK:       stream<<"ScaLAPACK";
                                break;
    default:
            assert(0);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "operator<<(std::ostream& stream, const ArrayDistribution& dist)";
    }

    if (dist._partitioningSchema == psLocalInstance)
    {
        stream<<" instance "<<dist._instanceId;
    }
    if (dist._distMapper.get() != NULL)
    {
        stream<<" "<<*dist._distMapper;
    }
    return stream;
}

/**
 * Implementation of SCATTER/GATHER method.
 */
void sync(NetworkManager* networkManager, const boost::shared_ptr<Query>& query, uint64_t instanceCount, bool isSendLocal=false)
{
    boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtSyncRequest);
    boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
    msg->setQueryID(query->getQueryID());
    networkManager->broadcastLogical(msg);

    if (isSendLocal) {
         networkManager->sendLocal(query, msg);
        ++instanceCount;
    }

    LOG4CXX_DEBUG(logger, "Sending sync to every one and waiting for " << instanceCount - 1 << " sync confirmations")
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->syncSG.enter(instanceCount - 1, ec);
    LOG4CXX_DEBUG(logger, "All confirmations received - continuing")
}


void barrier(uint64_t barrierId, NetworkManager* networkManager, const boost::shared_ptr<Query>& query, uint64_t instanceCount)
{
    boost::shared_ptr<MessageDesc> barrierMsg = boost::make_shared<MessageDesc>(mtBarrier);
    boost::shared_ptr<scidb_msg::DummyQuery> barrierRecord = barrierMsg->getRecord<scidb_msg::DummyQuery>();
    barrierMsg->setQueryID(query->getQueryID());
    barrierRecord->set_payload_id(barrierId);
    networkManager->broadcastLogical(barrierMsg);

    LOG4CXX_DEBUG(logger, "Sending barrier to every one and waiting for " << instanceCount - 1 << " barrier messages")
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->semSG[barrierId].enter(instanceCount - 1, ec);
    LOG4CXX_DEBUG(logger, "All barrier messages received - continuing")
}

/**
 * This can be used as a barrier mechanism across the cluster in a blocking/materializing operator
 * Note: redistributeXXX() uses the same mechanism.
 */
void syncBarrier(uint64_t barrierId, const boost::shared_ptr<Query>& query)
{
    LOG4CXX_DEBUG(logger, "syncBarrier: barrierId = " << barrierId);
    assert(query);
    NetworkManager* networkManager = NetworkManager::getInstance();
    assert(networkManager);
    const uint64_t instanceCount = query->getInstancesCount();
    assert(instanceCount>0);
    barrier(barrierId%MAX_BARRIERS, networkManager, query, instanceCount);
}

void syncSG(const boost::shared_ptr<Query>& query)
{
    assert(query);
    LOG4CXX_DEBUG(logger, "syncSG: queryID="<<query->getQueryID());
    NetworkManager* networkManager = NetworkManager::getInstance();
    assert(networkManager);
    const uint64_t instanceCount = query->getInstancesCount();
    assert(instanceCount>0);
    sync(networkManager, query, instanceCount, true);
}

/**
 * Compute hash over the groupby dimensions.
 * @param   allDims   Coordinates containing all the dims.
 * @param   isGroupby   For every dimension, whether it is a groupby dimension.
 *
 * @note The result can be larger than #instances!!! The caller should mod it.
 *
 */
InstanceID hashForGroupby(const Coordinates& allDims, const vector<bool>& isGroupby ) {
    assert(allDims.size()==isGroupby.size());
    Coordinates groups;
    for (size_t i=0; i<allDims.size(); ++i) {
        if (isGroupby[i]) {
            groups.push_back(allDims[i]);
        }
    }
    return VectorHash<Coordinate>()(groups);
}

/**
 * Compute the instanceID for a group.
 * For psGroupby, if the logic is modified, also change PhysicalQuantile.cpp::GroupbyQuantileArrayIterator::getInstanceForChunk.
 */
InstanceID getInstanceForChunk(boost::shared_ptr<Query> const& query,
                               Coordinates const& chunkPos,
                               ArrayDesc const& desc,
                               PartitioningSchema ps,
                               const boost::shared_ptr<DistributionMapper>& distMapper,
                               uint64_t instanceIdShift,
                               InstanceID destInstanceId,
                               PartitioningSchemaData* psData)
{
    static const char *funcName = "getInstanceForChunk: ";
    const uint64_t instanceCount = query->getInstancesCount();
    Dimensions const& dims = desc.getDimensions();
    const Coordinates *chunkPosition = &chunkPos;

    Coordinates mappedChunkPos;
    if (distMapper)
    {
        mappedChunkPos = distMapper->translate(chunkPos);
        chunkPosition = &mappedChunkPos;
    }

    switch (ps)
    {
    case psHashPartitioned:
    {
        destInstanceId = desc.getHashedChunkNumber((*chunkPosition)) % instanceCount;
        break;
    }
    case psByRow:
    {
        uint64_t dim0Length = dims[0].getLength();
        destInstanceId = ((*chunkPosition)[0] - dims[0].getStartMin()) / dims[0].getChunkInterval()
        / (((dim0Length + dims[0].getChunkInterval() - 1) / dims[0].getChunkInterval() + instanceCount - 1) / instanceCount);
        break;
    }
    case psByCol:
    {
        uint64_t dim1Length = dims.size() > 1 ? dims[1].getLength() : 0;
        if (dims.size() > 1)
        {
            destInstanceId = ((*chunkPosition)[1] - dims[1].getStartMin()) / dims[1].getChunkInterval()
            / (((dim1Length + dims[1].getChunkInterval() - 1) / dims[1].getChunkInterval() + instanceCount - 1) / instanceCount);
        }
        else
        {
            destInstanceId = 0;  //XXX TODO Tigor ; you wanted a comment because you wanted to look at this line
        }
        break;
    }
    case psScaLAPACK:
    {
        if ( dims.size() < 1 ||  // distribution is defined only for vectors
             dims.size() > 2) {  // and matrices, (not tensors, etc)
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_REDISTRIBUTE_ERROR);
        }
        PartitioningSchemaDataForScaLAPACK* repartInfo = dynamic_cast<PartitioningSchemaDataForScaLAPACK*>(psData);
        if (!repartInfo) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_REDISTRIBUTE_ERROR);
        }
        destInstanceId = repartInfo->getInstanceID((*chunkPosition), *(query.get()));
        break;
    }
    case psLocalInstance:
    {
        // essentially destInstanceId with a shift
        ASSERT_EXCEPTION((destInstanceId < instanceCount), funcName);
        break;
    }
    case psGroupby:
    {
        PartitioningSchemaDataGroupby* pIsGroupbyDim = dynamic_cast<PartitioningSchemaDataGroupby*>(psData);
        if (pIsGroupbyDim!=NULL) {
            destInstanceId = hashForGroupby((*chunkPosition), pIsGroupbyDim->_arrIsGroupbyDim);
        } else {
            stringstream ss;
            ss << funcName << "psGroupby";
            ASSERT_EXCEPTION(false, ss.str());
        }
        break;
    }
    case psReplication:
    {
        return ALL_INSTANCE_MASK;
    }
    case psUndefined:
    default:
    {
        stringstream ss;
        ss << funcName << "invalid distribution "<<ps;
        ASSERT_EXCEPTION(false, ss.str());
    }
    }
    return (destInstanceId + instanceIdShift) % instanceCount;
}

void sendToRemoteInstance(
        bool cachingLastEmptyBitmap,
        bool cachingReceivedChunks,
        bool isEmptyable,
        bool isEmptyIndicator,
        AttributeID attrId,
        shared_ptr<ConstRLEEmptyBitmap>& sharedEmptyBitmap,
        const ConstChunk& chunk,
        Coordinates const& coordinates,
        size_t& totalBytesSent,
        size_t& totalBytesSynced,
        shared_ptr<Query>& query,
        NetworkManager* networkManager,
        MessageType mt,
        InstanceID instanceID,
        size_t instanceCount,
        size_t networkBufferLimit
        )
{
    boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
    shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
    if (isEmptyable && !cachingLastEmptyBitmap && !cachingReceivedChunks && !isEmptyIndicator) {
        emptyBitmap = sharedEmptyBitmap;
        assert(emptyBitmap);
    }
    chunk.compress(*buffer, emptyBitmap);
    assert(buffer && buffer->getData());

    boost::shared_ptr<MessageDesc> chunkMsg = boost::make_shared<MessageDesc>(mt, buffer);
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
    chunkRecord->set_eof(false);
    chunkRecord->set_compression_method(buffer->getCompressionMethod());
    chunkRecord->set_attribute_id(attrId);
    chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
    chunkRecord->set_count(chunk.isCountKnown() ? chunk.count() : 0);
    chunkMsg->setQueryID(query->getQueryID());
    for (size_t i = 0; i < coordinates.size(); i++)
    {
        chunkRecord->add_coordinates(coordinates[i]);
    }

    networkManager->send(instanceID, chunkMsg);
    LOG4CXX_TRACE(logger, "Sending chunk with att=" << attrId << " to instance=" << instanceID);
    totalBytesSent += buffer->getDecompressedSize();
    if (totalBytesSent > totalBytesSynced + networkBufferLimit) {
        sync(networkManager, query, instanceCount);
        totalBytesSynced = totalBytesSent;
    }
}

void mergeToLocalInstance(
        bool isEmptyable,
        bool isEmptyIndicator,
        AttributeID attrId,
        shared_ptr<Query>& query,
        shared_ptr<Array> outputArray,
        vector<shared_ptr<ArrayIterator> >& outputIters,
        shared_ptr<ConstRLEEmptyBitmap>& sharedEmptyBitmap,
        const ConstChunk& chunk,
        Coordinates const& coordinates,
        vector <AggregatePtr> const& aggs,
        ArrayDesc const& desc
)
{
    ScopedMutexLock cs(query->resultCS);
    if (!outputIters[attrId]) {
        outputIters[attrId] = outputArray->getIterator(attrId);
    }
    if (outputIters[attrId]->setPosition(coordinates))
    {
        Chunk& dstChunk = outputIters[attrId]->updateChunk();
        if (aggs[attrId].get())
        {
            if (desc.getEmptyBitmapAttribute() == NULL)
            {
                dstChunk.nonEmptyableAggregateMerge(chunk, aggs[attrId], query);
            }
            else
            {
                dstChunk.aggregateMerge(chunk, aggs[attrId], query);
            }
        }
        else
        {
            dstChunk.merge(chunk, query);
        }
    }
    else
    {
        shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        if (isEmptyable && !isEmptyIndicator) {
            emptyBitmap = sharedEmptyBitmap;
        }
        outputIters[attrId]->copyChunk(chunk, emptyBitmap);
    }
    LOG4CXX_TRACE(logger, "Storing chunk with att=" << attrId << " locally")
}

AggregatePtr resolveAggregate(boost::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
                              Attributes const& inputAttributes,
                              AttributeID* inputAttributeID,
                              string* outputName)
{
    const shared_ptr<OperatorParam> &acParam = aggregateCall->getInputAttribute();

    try
    {
        if (PARAM_ASTERISK == acParam->getParamType())
        {
            AggregatePtr agg = AggregateLibrary::getInstance()->createAggregate( aggregateCall->getAggregateName(), TypeLibrary::getType(TID_VOID));

            if (inputAttributeID)
            {
                *inputAttributeID = INVALID_ATTRIBUTE_ID;
            }
            if (outputName)
            {
                *outputName = aggregateCall->getAlias().size() ? aggregateCall->getAlias() : agg->getName();
            }
            return agg;
        }
        else if (PARAM_ATTRIBUTE_REF == acParam->getParamType())
        {
            const shared_ptr<OperatorParamAttributeReference> &ref = (const shared_ptr<OperatorParamAttributeReference>&) acParam;

            AttributeDesc const& inputAttr = inputAttributes[ref->getObjectNo()];
            Type const& inputType = TypeLibrary::getType(inputAttr.getType());
            AggregatePtr agg = AggregateLibrary::getInstance()->createAggregate( aggregateCall->getAggregateName(), inputType);

            if (inputAttributeID)
            {
                *inputAttributeID = inputAttr.getId();
            }
            if (outputName)
            {
                *outputName = aggregateCall->getAlias().size() ? aggregateCall->getAlias() : inputAttr.getName() + "_" + agg->getName();
            }
            return agg;
        }
        else
        {
            // All other cases must have been thrown already during translation.
            assert(0);
        }
    }
    catch(const UserException &e)
    {
        if (SCIDB_LE_AGGREGATE_NOT_FOUND == e.getLongErrorCode())
        {
            throw CONV_TO_USER_QUERY_EXCEPTION(e, acParam->getParsingContext());
        }

        throw;
    }

    assert(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "resolveAggregate";
    return AggregatePtr();
}

void addAggregatedAttribute (
        boost::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
        ArrayDesc const& inputDesc,
        ArrayDesc& outputDesc,
        bool operatorDoesAggregationInOrder)
{
    string outputName;

    AggregatePtr agg = resolveAggregate(aggregateCall, inputDesc.getAttributes(), 0, &outputName);

    if ( !operatorDoesAggregationInOrder && agg->isOrderSensitive() ) {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_AGGREGATION_ORDER_MISMATCH) << agg->getName();
    }

    outputDesc.addAttribute( AttributeDesc(outputDesc.getAttributes().size(),
                                           outputName,
                                           agg->getResultType().typeId(),
                                           AttributeDesc::IS_NULLABLE,
                                           0));
}

/**
 * Record the array 't' in the transient array cache. Implements a callback
 * that is suitable for use as a query finalizer.
 */
static void recordTransient(const MemArrayPtr& t,const QueryPtr& query)
{
    if (query->wasCommitted())                           // Was committed ok?
    {
        transient::record(t);                            // ...record in cache
    }
}

boost::shared_ptr<Array> redistribute(boost::shared_ptr<Array> inputArray,
                                      boost::shared_ptr<Query> query,
                                      PartitioningSchema ps,
                                      const string& resultArrayName,
                                      InstanceID instanceID,
                                      boost::shared_ptr<DistributionMapper> distMapper,
                                      size_t shift,
                                      PartitioningSchemaData* psData)
{
    LOG4CXX_DEBUG(logger, "SG started with partitioning schema = " << ps << ", instanceID = " << instanceID)

    uint64_t totalBytesSent = 0;
    uint64_t totalBytesSynced = 0;
    size_t networkBufferLimit = std::abs(Config::getInstance()->getOption<int>(CONFIG_NETWORK_BUFFER))*MiB;

    /**
     * Creating result array with the same descriptor as the input one
     */
    NetworkManager* networkManager = NetworkManager::getInstance();
    const uint64_t instanceCount = query->getInstancesCount();
    const InstanceID myInstanceID = query->getInstanceID();

    assert(instanceID == COORDINATOR_INSTANCE_MASK ||
           instanceID == ALL_INSTANCE_MASK ||
           instanceID < query->getInstancesCount());

    assert(ps != psLocalInstance || instanceID != ALL_INSTANCE_MASK);

    if (ps == psLocalInstance || ps == psReplication) {
        networkBufferLimit /= instanceCount;
    }

    if (instanceID == COORDINATOR_INSTANCE_MASK) {
        instanceID = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
    }
    if (instanceCount == 1 || (ps == psLocalInstance && instanceID == ALL_INSTANCE_MASK)) {
        return inputArray;
    }

    ArrayDesc const& desc = inputArray->getArrayDesc();
    Dimensions const& srcDims = desc.getDimensions();
    size_t nDims = srcDims.size();

    size_t nAttrs = desc.getAttributes().size();
    assert(nAttrs>0);
    bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);
    if (isEmptyable && desc.getEmptyBitmapAttribute()->getId() != nAttrs-1) {
        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_REDISTRIBUTE_ERROR1);
    }

    // For now we do not cache empty bitmaps or received chunks in redistribute()
    bool cachingLastEmptyBitmap = false;
    bool cachingReceivedChunks = false;

    shared_ptr<SGChunkReceiver> chunkReceiver = shared_ptr<SGChunkReceiver>(
            new SGChunkReceiver(cachingLastEmptyBitmap, cachingReceivedChunks, desc, query));

    boost::shared_ptr<Array> outputArray;
    ArrayID resultArrayId = 0;
    PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(nDims);
    boost::shared_ptr<JobQueue> incomingQueue;

    if (resultArrayName.empty())
    {
        outputArray = make_shared<MemArray>(desc, query);

        LOG4CXX_DEBUG(logger, "Temporary array was opened")
    }
    else
    {
        ArrayDesc outputDesc;
        SystemCatalog::getInstance()->getArrayDesc(resultArrayName, outputDesc, false);

        if (outputDesc.isTransient())
        {
            outputArray.reset(new MemArray(outputDesc,query));
            resultArrayId = outputDesc.getUAId();
            query->pushFinalizer(bind(&recordTransient,outputArray,_1));
        }
        else
        {
            outputArray = boost::shared_ptr<Array>(DBArray::newDBArray(resultArrayName, query));
            resultArrayId = outputArray->getHandle();
            query->getReplicationContext()->enableInboundQueue(resultArrayId, outputArray);
            incomingQueue = PhysicalOperator::getGlobalQueueForOperators();
            LOG4CXX_DEBUG(logger, "Array " << resultArrayName << " was opened")
         }
    }

    ArrayDesc const& dstDesc = outputArray->getArrayDesc();

    ASSERT_EXCEPTION( (!query->getOperatorContext()), "SG");

    barrier(0, networkManager, query, instanceCount);
    /**
     * Assigning result of this operation for current query and signal to concurrent handlers that they
     * can continue to work (after the barrier)
     */
    shared_ptr<RedimInfo> dummyRedimInfo;
    shared_ptr<SGContext> sgCtx = shared_ptr<SGContext>(
            new SGContext(chunkReceiver, dummyRedimInfo, outputArray, std::vector<AggregatePtr>() ));
    query->setOperatorContext(sgCtx, incomingQueue);

    /**
     *  Sending out our parts of the input array
     */
    vector<boost::shared_ptr<ConstArrayIterator> > inputIters;

    for (AttributeID attrId = 0; attrId < nAttrs; attrId++)
    {
        inputIters.push_back(inputArray->getConstIterator(attrId));
    }

    // we want to set outputIters lazily, after getting mutex.
    // Here we just allocate placeholders.
    vector<boost::shared_ptr<ArrayIterator> > outputIters(nAttrs);

    while (!inputIters[0]->end())
    {
        Coordinates chunkPosition = inputIters[0]->getPosition();
        instanceID = getInstanceForChunk(query, chunkPosition,desc,ps,distMapper,shift,instanceID, psData);

        // sharedEmptyBitmap is the empty bitmap at a given chunkPos, to be shared by all the attributes.
        shared_ptr<ConstRLEEmptyBitmap> sharedEmptyBitmap;

        const ConstChunk& firstAttrChunk = inputIters[0]->getChunk();

        // Check if the chunks have any values
        bool chunkHasElems = (!isEmptyable) || (!firstAttrChunk.isEmpty());

        for (AttributeID attrId = nAttrs; attrId-- != 0;)
        {
            query->validate();

            // XXX TODO:
            // Calling getChunk() on the input iterators causes problems
            // (the chunk gets corrupted).  It was observed on queries involving
            // subarray() (which is redistributed downstream).
            const ConstChunk& chunk = (attrId==0) ? firstAttrChunk :
                                      inputIters[attrId]->getChunk();

            if(chunkHasElems && chunk.getSize() > 0) {

                if (resultArrayId != 0 && attrId == nAttrs - 1) {
                    bounds.updateFromChunk(&chunk, dstDesc.getEmptyBitmapAttribute() == NULL);
                }

                AttributeDesc const& attributeDesc = chunk.getAttributeDesc();
                assert(attributeDesc.getId() == attrId);

                const Coordinates& coordinates = chunk.getFirstPosition(false);

                boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
                if (isEmptyable) {
                    if (!sharedEmptyBitmap) {
                        assert(nAttrs == attrId+1);
                        sharedEmptyBitmap = chunk.getEmptyBitmap();
                    } else {
                        assert(nAttrs > attrId+1);
                    }
                    if (!attributeDesc.isEmptyIndicator()) {
                        emptyBitmap = sharedEmptyBitmap;
                    }
                    assert(chunkHasElems != chunk.isEmpty());
                }

                // Sending current chunk to instanceID instance or saving locally

                if ((InstanceID)instanceID != myInstanceID || ps == psReplication)
                {
                    boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
                    chunk.compress(*buffer, emptyBitmap);
                    boost::shared_ptr<MessageDesc> chunkMsg = boost::make_shared<MessageDesc>(mtChunk, buffer);
                    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
                    chunkRecord->set_eof(false);
                    chunkRecord->set_compression_method(buffer->getCompressionMethod());
                    chunkRecord->set_attribute_id(attrId);
                    chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
                    chunkRecord->set_count(chunk.isCountKnown() ? chunk.count() : 0);
                    chunkMsg->setQueryID(query->getQueryID());
                    for (size_t i = 0; i < coordinates.size(); i++) {
                        chunkRecord->add_coordinates(coordinates[i]);
                    }

                    if (ps != psReplication) {
                        networkManager->send(instanceID, chunkMsg);
                        LOG4CXX_TRACE(logger, "Sending chunk with att=" << attrId << " to instance=" << instanceID)
                    }
                    else {
                        networkManager->broadcastLogical(chunkMsg);
                        LOG4CXX_TRACE(logger, "Sending out chunk with att=" << attrId << " to every instance")
                    }
                    totalBytesSent += buffer->getDecompressedSize();
                    if (totalBytesSent > totalBytesSynced + networkBufferLimit) {
                        sync(networkManager, query, instanceCount);
                        totalBytesSynced = totalBytesSent;
                    }
                }
                if ((InstanceID)instanceID == myInstanceID || ps == psReplication)
                {
                    ScopedMutexLock cs(query->resultCS);
                    if(sgCtx->_targetVersioned)
                    {
                        sgCtx->_newChunks.insert(coordinates);
                    }
                    assert(attrId < nAttrs);
                    if (!outputIters[attrId]) {
                        outputIters[attrId] = outputArray->getIterator(attrId);
                    }
                    if (outputIters[attrId]->setPosition(coordinates)) {
                        Chunk& dstChunk = outputIters[attrId]->updateChunk();
                        if (dstChunk.isReadOnly()) {
                            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_CANT_MERGE_READONLY_CHUNK);
                        }
                        dstChunk.merge(chunk, query);
                    } else {
                        outputIters[attrId]->copyChunk(chunk, emptyBitmap);
                    }
                }
            }
            ++(*inputIters[attrId]);
        }
    }

    sync(networkManager, query, instanceCount);
    barrier(1, networkManager, query, instanceCount);
    LOG4CXX_DEBUG(logger, "SG termination barrier reached.");

    /**
     * Reset SG Context to NULL
     */
    query->unsetOperatorContext();

    sgCtx->runCallback();

    if (resultArrayId != 0) {
        SystemCatalog::getInstance()->updateArrayBoundaries(dstDesc, bounds);

        if (!dstDesc.isTransient())
        {
            if (sgCtx->_targetVersioned)
            {   //storing sg and array is mutable - insert tombstones:
                StorageManager::getInstance().removeDeadChunks(outputArray->getArrayDesc(), sgCtx->_newChunks, query);
            }

            // XXX TODO: at this point the replicas can still be arriving to this instance
            // so the flush is a bit premature.
            query->getReplicationContext()->replicationSync(resultArrayId);
            query->getReplicationContext()->removeInboundQueue(resultArrayId);

            StorageManager::getInstance().flush();
        }
    }
    LOG4CXX_DEBUG(logger, "Finishing SCATTER/GATHER work; sent " << totalBytesSent << " bytes.");
    return outputArray;
}

PhysicalBoundaries findArrayBoundaries(shared_ptr<Array> srcArray,
                                       boost::shared_ptr<Query> query,
                                       bool global)
{
    ArrayDesc const& srcDesc = srcArray->getArrayDesc();
    ArrayID id = srcDesc.getId();
    try
    {
        Coordinates lo = SystemCatalog::getInstance()->getLowBoundary(id);
        Coordinates hi = SystemCatalog::getInstance()->getHighBoundary(id);
        return PhysicalBoundaries(lo,hi);

    } catch (SystemException const& x) {
        if (x.getLongErrorCode() != SCIDB_LE_ARRAYID_DOESNT_EXIST) {
            throw;
        }
    }

    size_t nDims = srcDesc.getDimensions().size();
    Coordinates lo(nDims, MAX_COORDINATE), hi(nDims, MIN_COORDINATE);
    {
        shared_ptr<ConstArrayIterator> arrayIterator = srcArray->getConstIterator(srcDesc.getAttributes()[0].getId());
        while (!arrayIterator->end())
        {
            {
                ConstChunk const& chunk = arrayIterator->getChunk();
                Coordinates chunkLo = chunk.getFirstPosition(false);
                Coordinates chunkHi = chunk.getLastPosition(false);
                for (size_t i=0; i< lo.size(); i++)
                {
                    if (chunkLo[i] < lo[i])
                    {   lo[i]=chunkLo[i]; }
                    if (chunkHi[i] > hi[i])
                    {   hi[i]=chunkHi[i]; }
                }
            }
            ++(*arrayIterator);
        }
    }

    PhysicalBoundaries localBoundaries(lo,hi);

    if (global)
    {
        NetworkManager* networkManager = NetworkManager::getInstance();
        const size_t nInstances = (size_t)query->getInstancesCount();
        const size_t myInstanceId = (size_t)query->getInstanceID();

        shared_ptr<SharedBuffer> buf;
        if (myInstanceId != 0)
        {
            networkManager->send(0, localBoundaries.serialize(), query);
            localBoundaries = PhysicalBoundaries::deSerialize(networkManager->receive(0, query));
        }
        else
        {
            for (size_t instance = 1; instance < nInstances; instance++)
            {
                PhysicalBoundaries receivedBoundaries = PhysicalBoundaries::deSerialize(networkManager->receive(instance,query));
                localBoundaries = localBoundaries.unionWith(receivedBoundaries);
            }

            for (size_t instance = 1; instance < nInstances; instance++)
            {
                networkManager->send(instance, localBoundaries.serialize(), query);
            }
        }
    }

    return localBoundaries;
}

void PhysicalBoundaries::updateFromChunk(ConstChunk const* chunk, bool chunkShapeOnly)
{
    size_t nDims = _startCoords.size();
    if (chunk == NULL)
    {   return; }

    //chunk iteration is expensive - only perform if needed
    Coordinates const& chunkFirstPos = chunk->getFirstPosition(false), chunkLastPos = chunk->getLastPosition(false);
    bool updateLowBound = false, updateHiBound = false;
    for (size_t i = 0; i < nDims; i++)
    {
       if (chunkFirstPos[i] < _startCoords[i])
       {
           if ( chunkShapeOnly )
           {    _startCoords[i] = chunkFirstPos[i]; }
           else
           {   updateLowBound = true; }
       }

       if (chunkLastPos[i] > _endCoords[i])
       {
           if ( chunkShapeOnly)
           {    _endCoords[i] = chunkLastPos[i]; }
           else
           {    updateHiBound = true; }
       }
    }

    //The chunk is inside the box and cannot expand the bounds. Early exit.
    if (!updateLowBound && !updateHiBound)
    {   return; }

    //TODO: there is a further optimization opportunity here. The given chunk *should* always be a bitmap
    //chunk. Once we verify that, we can iterate over bitmap segments and compute coordinates. Committing
    //this due to timing constraints - should revisit and optimize this when possible.
    boost::shared_ptr<ConstChunkIterator> citer = chunk->materialize()->getConstIterator();
    while (!citer->end() && (updateLowBound || updateHiBound))
    {
        Coordinates const& pos = citer->getPosition();
        bool updated = false;
        for (size_t j = 0; j < nDims; j++)
        {
            if (updateHiBound && pos[j] > _endCoords[j])
            {
                _endCoords[j] = pos[j];
                updated = true;
            }
            if (updateLowBound && pos[j] < _startCoords[j])
            {
                _startCoords[j] = pos[j];
                updated = true;
            }
        }
        if(updated) //it's likely that no further update can come from this chunk
        {
            if(updateHiBound)
            {
                size_t k=0;
                while (k<nDims && _endCoords[k]>=chunkLastPos[k])
                { k++; }
                if (k==nDims) //no more useful data for hi bound could come from this chunk!
                {   updateHiBound=false; }
            }
            if(updateLowBound)
            {
                size_t k=0;
                while (k<nDims && _startCoords[k]<=chunkFirstPos[k])
                { k++; }
                if (k==nDims) //no more useful data for lo bound could come from this chunk!
                {   updateLowBound=false; }
            }
        }
        ++(*citer);
    }
}

PhysicalBoundaries PhysicalBoundaries::trimToDims(Dimensions const& dims) const
{
    SCIDB_ASSERT(_startCoords.size() == dims.size());
    size_t nDims = dims.size();

    Coordinates resStart(nDims);
    Coordinates resEnd(nDims);

    for(size_t i=0; i<nDims; i++)
    {
        resStart[i] = std::max<Coordinate> (dims[i].getStartMin(), _startCoords[i]);
        resEnd[i] = std::min<Coordinate> (dims[i].getEndMax(), _endCoords[i]);
    }

    return PhysicalBoundaries(resStart, resEnd, _density);
}

PhysicalBoundaries PhysicalBoundaries::createFromChunkList(boost::shared_ptr<Array>& inputArray,
                                                           const std::set<Coordinates, CoordinatesLess>& chunkCoordinates)
{
    ASSERT_EXCEPTION((inputArray->getSupportedAccess() == Array::RANDOM),
                     "PhysicalBoundaries::createFromChunkList: ");

    const ArrayDesc& arrayDesc = inputArray->getArrayDesc();
    const size_t nDims = arrayDesc.getDimensions().size();

    if (chunkCoordinates.empty()) {
        return PhysicalBoundaries::createEmpty(nDims);
    }

    typedef set<Coordinates, CoordinatesLess> ChunkCoordinates;
    typedef boost::unordered_set<const Coordinates*> CoordHashSet;
    CoordHashSet chunksToExamine;

    if (nDims>1) {

        typedef std::vector<std::pair<int64_t, std::deque<const Coordinates*> > > ChunkListByExtremeDim;
        ChunkListByExtremeDim chunksWithMinDim(nDims,make_pair(MAX_COORDINATE,std::deque<const Coordinates*>()));
        assert(nDims == chunksWithMinDim.size());
        ChunkListByExtremeDim chunksWithMaxDim(nDims,make_pair(MIN_COORDINATE,std::deque<const Coordinates*>()));
        assert(nDims == chunksWithMaxDim.size());

        // find all boundary chunks
        for (ChunkCoordinates::const_iterator iter = chunkCoordinates.begin();
             iter != chunkCoordinates.end(); ++iter) {
            const Coordinates& chunkCoords = *iter;
            assert(chunkCoords.size() == nDims);
            for (size_t d=0; d < nDims; ++d) {
                pair<int64_t, std::deque<const Coordinates*> >& minChunks = chunksWithMinDim[d];
                if (chunkCoords[d] < minChunks.first) {
                    minChunks.second.clear();
                    minChunks.first = chunkCoords[d];
                    minChunks.second.push_back(&chunkCoords);
                }  else if (chunkCoords[d] == minChunks.first) {
                    minChunks.second.push_back(&chunkCoords);
                } else { }

                pair<int64_t, std::deque<const Coordinates*> >& maxChunks = chunksWithMaxDim[d];
                if (chunkCoords[d] > maxChunks.first) {
                    maxChunks.second.clear();
                    maxChunks.first = chunkCoords[d];
                    maxChunks.second.push_back(&chunkCoords);
                }  else if (chunkCoords[d] == maxChunks.first) {
                    maxChunks.second.push_back(&chunkCoords);
                } else { }
            }
        }

        // throw away duplicates
        for (size_t d=0; d < nDims; ++d) {
            std::deque<const Coordinates*>& minList = chunksWithMinDim[d].second;
            for (std::deque<const Coordinates*>::const_iterator iter = minList.begin();
                 iter != minList.end(); ++iter) {
                chunksToExamine.insert(*iter).second;
            }

            std::deque<const Coordinates*>& maxList = chunksWithMaxDim[d].second;
            for (std::deque<const Coordinates*>::const_iterator iter = maxList.begin();
                 iter != maxList.end(); ++iter) {
                chunksToExamine.insert(*iter).second;
            }
        }
    } else {
        assert(nDims==1);
        const Coordinates& firstCoord = *chunkCoordinates.begin();
        chunksToExamine.insert(&firstCoord);
        const Coordinates& lastCoord = *(--chunkCoordinates.end());
        chunksToExamine.insert(&lastCoord);
    }

    // update bounds using the boundary chunks
    PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(nDims);

    const AttributeID attr = arrayDesc.getAttributes().size() - 1;
    const bool isNoEmptyTag = (arrayDesc.getEmptyBitmapAttribute() == NULL);
    shared_ptr<ConstArrayIterator> arrayIter = inputArray->getConstIterator(attr);
    for (CoordHashSet::const_iterator iter = chunksToExamine.begin();
         iter != chunksToExamine.end(); ++iter) {
        const Coordinates* chunkCoords = *iter;
        bool rc = arrayIter->setPosition(*chunkCoords);
        SCIDB_ASSERT(rc);
        const ConstChunk& chunk = arrayIter->getChunk();
        bounds.updateFromChunk(&chunk, isNoEmptyTag);
    }
    return bounds;
}

void BaseLogicalOperatorFactory::registerFactory()
{
    OperatorLibrary::getInstance()->addLogicalOperatorFactory(this);
}

void BasePhysicalOperatorFactory::registerFactory()
{
    OperatorLibrary::getInstance()->addPhysicalOperatorFactory(this);
}

VersionID OperatorParamArrayReference::getVersion() const
{
    return _version;
}

void LogicalOperator::inferArrayAccess(boost::shared_ptr<Query>& query)
{
    for (size_t i=0, end=_parameters.size(); i<end; ++i) {
        const shared_ptr<OperatorParam>& param = _parameters[i];
        string arrayName;
        if (param->getParamType() == PARAM_ARRAY_REF) {
            arrayName = ((boost::shared_ptr<OperatorParamReference>&)param)->getObjectName();
        } else if (param->getParamType() == PARAM_SCHEMA) {
            arrayName = ((boost::shared_ptr<OperatorParamSchema>&)param)->getSchema().getName();
        }
        if (arrayName.empty()) {
            continue;
        }
        string baseName = arrayName.substr(0, arrayName.find('@'));
        shared_ptr<SystemCatalog::LockDesc> lock(new SystemCatalog::LockDesc(baseName,
        query->getQueryID(),
        Cluster::getInstance()->getLocalInstanceId(),
        SystemCatalog::LockDesc::COORD,
        SystemCatalog::LockDesc::RD));
        query->requestLock(lock);
    }
}

InjectedErrorListener<OperatorInjectedError> PhysicalOperator::_injectedErrorListener;

boost::shared_ptr<ThreadPool> PhysicalOperator::_globalThreadPoolForOperators;
boost::shared_ptr<JobQueue> PhysicalOperator::_globalQueueForOperators;
Mutex PhysicalOperator::_mutexGlobalQueueForOperators;

boost::shared_ptr<JobQueue> PhysicalOperator::getGlobalQueueForOperators()
{
    ScopedMutexLock cs(_mutexGlobalQueueForOperators);
    if (!_globalThreadPoolForOperators) {
        _globalQueueForOperators = boost::shared_ptr<JobQueue>(new JobQueue());
        _globalThreadPoolForOperators = boost::shared_ptr<ThreadPool>(
                new ThreadPool(Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_THREADS), _globalQueueForOperators));
        _globalThreadPoolForOperators->start();
    }
    return _globalQueueForOperators;
}

void PhysicalOperator::repartByLeftmost(vector<ArrayDesc> const& inputSchemas,
                                        vector<ArrayDesc const*>& repartPtrs) const
{
    const size_t N = inputSchemas.size();
    assert(N > 1); // ... else you are calling the wrong canned implementation.
    assert(N == repartPtrs.size());

    _repartSchemas.clear();
    const size_t nDims = inputSchemas[0].getDimensions().size();
    ArrayDesc const& leftMost = inputSchemas[0];
    repartPtrs[0] = 0;       // Do not repartition leftmost input array.

    for (size_t i = 1; i < N; ++i) {
        ArrayDesc const& thisSchema = inputSchemas[i];
        assert(thisSchema.getDimensions().size() == nDims);

        if (samePartitioning(leftMost, thisSchema)) {
            // Already has right chunkSize and overlap, do nothing.
            repartPtrs[i] = 0;
        } else {
            // Clone this schema and adjust dimensions according to leftmost.
            Attributes attrs = thisSchema.getAttributes();
            Dimensions const& srcDims = leftMost.getDimensions();
            Dimensions dims(thisSchema.getDimensions());
            for (size_t j = 0; j < nDims; ++j) {
                dims[j].setChunkInterval(srcDims[j].getChunkInterval());

                // Take smallest overlap since we can't (easily) conjure up cells that aren't there.
                dims[j].setChunkOverlap(min(srcDims[j].getChunkOverlap(),
                                            dims[j].getChunkOverlap()));
            }

            _repartSchemas.push_back(make_shared<ArrayDesc>(thisSchema.getName(), attrs, dims));
            repartPtrs[i] = _repartSchemas.back().get();
        }
    }
    if (_repartSchemas.empty()) {
        // Assertions elsewhere hate an all-NULLs vector here.
        repartPtrs.clear();
    }
}

void StoreJob::run()
{
    ArrayDesc const& dstArrayDesc = _dstArray->getArrayDesc();
    size_t nAttrs = dstArrayDesc.getAttributes().size();
    Query::setCurrentQueryID(_query->getQueryID());

    for (size_t i = _shift; i != 0 && !_srcArrayIterators[0]->end(); --i)
    {
        for (size_t j = 0; j < nAttrs; j++)
        {
            ++(*_srcArrayIterators[j]);
        }
    }

    while (!_srcArrayIterators[0]->end())
    {
        bool chunkHasElems(true);
        for (size_t i = 0; i < nAttrs; i++)
        {
            ConstChunk const& srcChunk = _srcArrayIterators[i]->getChunk();
            if (i==0) {
                chunkHasElems = hasValues(srcChunk);
                if (chunkHasElems) {
                    createdChunks.insert(_srcArrayIterators[0]->getPosition());
                }
            } else {
                assert(chunkHasElems == hasValues(srcChunk));
            }
            if (chunkHasElems) {
                if (i == nAttrs - 1)
                {
                    bounds.updateFromChunk(&srcChunk, dstArrayDesc.getEmptyBitmapAttribute() == NULL);
                }
                _dstArrayIterators[i]->copyChunk(srcChunk);
            }
            Query::validateQueryPtr(_query);
            for (size_t j = _step; j != 0 && !_srcArrayIterators[i]->end(); ++(*_srcArrayIterators[i]), --j);
        }
    }
}

bool StoreJob::hasValues(ConstChunk const& srcChunk)
{
    ArrayDesc const& srcArrayDesc = _srcArray->getArrayDesc();
    bool isSrcEmptyable = (srcArrayDesc.getEmptyBitmapAttribute() != NULL);

    // XXX TODO: until we have the single RLE? data format,
    //           we need to filter out other depricated formats (e.g. dense/sparse/nonempyable)
    bool chunkHasVals = (!isSrcEmptyable) || (!srcChunk.isEmpty());
    return chunkHasVals;
}

} //namespace
