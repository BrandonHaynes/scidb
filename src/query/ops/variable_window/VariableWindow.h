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
 * VariableWindow.h
 *  Created on: Feb 9, 2011
 *      Author: poliocough@gmail.com
 */

#include <deque>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include "query/Operator.h"

namespace scidb
{

struct AggregatedValue
{
    AggregatedValue(Coordinate const& crd, InstanceID const& nid, size_t nAggs):
        coord(crd), instanceId(nid),  vals(nAggs)
    {}

    Coordinate coord;
    uint32_t instanceId;
    vector<Value> vals;
};

class WindowEdge
{
private:
    //Replace this with new object CBRLEPayload
    std::deque<Value> _values;
    std::deque<Coordinate> _valueCoords;
    std::deque<uint32_t> _instanceIDs;
    uint32_t _numFollowing;

public:
    WindowEdge(): _values(0), _valueCoords(0), _instanceIDs(0), _numFollowing(0)
    {}

    virtual ~WindowEdge()
    {}

    inline size_t getNumValues() const
    {
        return _values.size();
    }

    inline size_t getNumCoords() const
    {
        return _valueCoords.size();
    }

    inline void clearCoords() 
    {
        _valueCoords.clear();
        _instanceIDs.clear();
    }

    inline void addPreceding(Value const& v)
    {
        assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
        _values.push_back(v);
    }

    inline void addCentral(Value const& v, Coordinate const& coord, InstanceID const& nid)
    {
        assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
        if(_valueCoords.size())
        {
            _numFollowing++;
        }
        _values.push_back(v);
        _valueCoords.push_back(coord);
        _instanceIDs.push_back(nid);
    }

    inline void addFollowing(Value const& v)
    {
        assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
        _values.push_back(v);
        _numFollowing++;
    }

    inline size_t getNumFollowing() const
    {
        return _numFollowing;
    }

    inline size_t getNumFinalFollowing() const
    {
        if(_valueCoords.size())
        {
            return _numFollowing - _valueCoords.size() + 1;
        }
        else
        {
            //if there are no coords, there can't be any following
            assert(_numFollowing == 0);
            return 0;
        }
    }

    inline void addLeftEdge(shared_ptr<WindowEdge> const& leftEdge)
    {
        assert(leftEdge.get());
        if (_valueCoords.size())
        {
            _numFollowing += leftEdge->_values.size();
        }
        else
        {
            _numFollowing = leftEdge->_numFollowing;
        }

        _values.insert(_values.end(), leftEdge->_values.begin(), leftEdge->_values.end());
        _valueCoords.insert(_valueCoords.end(), leftEdge->_valueCoords.begin(), leftEdge->_valueCoords.end());
        _instanceIDs.insert(_instanceIDs.end(), leftEdge->_instanceIDs.begin(), leftEdge->_instanceIDs.end());

    }
 
    inline shared_ptr<WindowEdge> split(size_t nPreceding, size_t nFollowing)
    {
        assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size() && _values.size() == nPreceding + nFollowing);

        shared_ptr<WindowEdge> newEdge ( new WindowEdge());
        newEdge->_values.assign(_values.begin(), _values.end());
        newEdge->_valueCoords.assign(_valueCoords.begin() + nPreceding, _valueCoords.end());
        newEdge->_instanceIDs.assign(_instanceIDs.begin() + nPreceding, _instanceIDs.end());
        if(newEdge->_valueCoords.size())
        {
            newEdge->_numFollowing = newEdge->_valueCoords.size() -1;
        }
        else
        {
            newEdge->_numFollowing =0;
        }

        _valueCoords.erase(_valueCoords.begin() + nPreceding, _valueCoords.end());
        _instanceIDs.erase(_instanceIDs.begin() + nPreceding, _instanceIDs.end());
        if(_valueCoords.size())
        {
            _numFollowing = _values.size()-1;
        }
        else
        {
            _numFollowing = _values.size();
        }
        return newEdge;
    }

    inline boost::shared_ptr<AggregatedValue> churn (size_t numPreceding, size_t numFollowing, vector<AggregatePtr> const& aggs)
    {
        assert(_instanceIDs.size() > 0 && _instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
        if(_instanceIDs.size() == 0 || _valueCoords.size()==0 || _values.size() == 0 || _instanceIDs.size() != _valueCoords.size())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Incorrect churn call";
        }

        boost::shared_ptr<AggregatedValue> result (new AggregatedValue(_valueCoords.front(), _instanceIDs.front(), aggs.size()));
        size_t currentPreceding = (size_t) std::max<int64_t>( (int64_t) (_values.size() - _numFollowing - 1), 0);
        assert(currentPreceding <= numPreceding); //yes; otherwise the result won't be centered around the right coordinate
        size_t windowSize = currentPreceding + std::min<size_t>((size_t)_numFollowing, numFollowing) + 1;
        assert(windowSize <= _values.size());

//TODO: make this whole class an extension of ConstRLEPayload where you can add to back and remove from front.
//        RLEPayload::append_iterator iter(bitSize);
//        for (size_t i=0; i<windowSize; i++)
//        {
//            iter.add(_values[i]);
//        }
//        iter.flush();
//RIDICULOUS:
//        boost::scoped_ptr<RLEPayload>payload(iter.getPayload());
//        agg->accumulateIfNeeded(state, payload.get());
//        payload.reset();


        for(size_t h =0; h<aggs.size(); h++)
        {
            Value state(aggs[h]->getStateType());
            aggs[h]->initializeState(state);
            for (size_t i=0; i<windowSize; i++)
            {
                aggs[h]->accumulateIfNeeded(state, _values[i]);
            }
            aggs[h]->finalResult(result->vals[h], state);
        }

        _valueCoords.pop_front();
        _instanceIDs.pop_front();
        if(_values.size() - _numFollowing > numPreceding)
        {
            _values.pop_front();
        }
        if(_numFollowing>0)
        {
            _numFollowing --;
        }

        return result;
    }

    inline void clear()
    {
        assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
        _values.clear();
        _valueCoords.clear();
        _instanceIDs.clear();
        _numFollowing=0;
    }

    //Marshalling scheme: [nCOORDS][nFollowing][COORDS][INSTANCEIDS][nVALS][VAL1SIZE][VAL1][-VAL2MC][VAL3SIZE][VAL3]...
    //Each value is preceded by VALSIZE or VALMC. If the value is negative - it is the missing code.
    inline size_t getBinarySize()
    {
        assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());

        //for nCOORDS, nVALS, numFollowing
        size_t result = 3 * sizeof(size_t);

        if(_valueCoords.size())
        {
            result += (sizeof(InstanceID) + sizeof(Coordinate)) * _valueCoords.size();
        }
        if(_values.size())
        {
            for(size_t i =0; i<_values.size(); i++)
            {
                //for VALSIZE or VALMC
                result += sizeof(int64_t);
                if(_values[i].isNull()==false)
                {
                    result += _values[i].size();
                }
            }
        }
        return result;
    }

    inline Coordinate getNextCoord() const
    {
        assert(_valueCoords.size() > 0);
        return _valueCoords.front();
    }

    inline char* marshall (char* buf)
    {
        assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
        size_t* numCoords = (size_t*) buf;
        *numCoords=_valueCoords.size();
        numCoords++;
        *numCoords=_numFollowing;
        numCoords++;
        Coordinate* coordPtr = (Coordinate*) numCoords;
        for(size_t i=0; i<_valueCoords.size(); i++)
        {
            *coordPtr = _valueCoords[i];
            coordPtr++;
        }
        InstanceID* instancePtr = (InstanceID*) coordPtr;
        for(size_t i=0; i<_instanceIDs.size(); i++)
        {
            *instancePtr = _instanceIDs[i];
            instancePtr++;
        }
        size_t* numVals = (size_t*) instancePtr;
        *numVals = _values.size();
        numVals++;
        int64_t* valSizePtr = (int64_t*) numVals;
        for (size_t i=0; i<_values.size(); i++)
        {
            if (_values[i].isNull())
            {
                *valSizePtr = _values[i].getMissingReason() * -1;
                valSizePtr++;
            }
            else
            {
                size_t size = _values[i].size();
                *valSizePtr = size;
                char* data = (char*) (valSizePtr+1);
                memcpy(data, _values[i].data(), size);
                valSizePtr = (int64_t*) (data+size);
            }
        }
        return (char*) valSizePtr;
    }

    inline char const* unMarshall(char const* buf)
    {
        size_t* numCoordsPtr = (size_t*) buf;
        size_t numCoords = *numCoordsPtr;
        numCoordsPtr++;
        _numFollowing+= (*numCoordsPtr);
        Coordinate* coordPtr = (Coordinate*)(numCoordsPtr+1);
        for(size_t i=0; i<numCoords; i++)
        {
            Coordinate coord = *coordPtr;
            _valueCoords.push_back(coord);
            coordPtr++;
        }
        InstanceID* nPtr = (InstanceID*) coordPtr;
        for(size_t i=0; i<numCoords; i++)
        {
            InstanceID nid = *nPtr;
            _instanceIDs.push_back(nid);
            nPtr++;
        }
        size_t* numValsPtr = (size_t*) nPtr;
        size_t numVals = *numValsPtr;
        int64_t* valSizePtr = (int64_t*)(numValsPtr+1);
        for(size_t i=0; i<numVals; i++)
        {
            int64_t valSize = *valSizePtr;
            Value val;
            if(valSize<=0)
            {
                val.setNull( (int) (valSize*-1) );
                valSizePtr++;
            }
            else
            {
                char* dataPtr=(char*) (valSizePtr+1);
                val.setData(dataPtr, valSize);
                valSizePtr = (int64_t*) (dataPtr+valSize);
            }
            _values.push_back(val);
        }
        return (char*) (valSizePtr);
    }

    bool operator == (const WindowEdge & other ) const
    {
        if (_values.size() != other._values.size() ||
            _valueCoords.size() != other._valueCoords.size() ||
            _instanceIDs.size() != other._instanceIDs.size() ||
            _numFollowing != other._numFollowing)
        {
            return false;
        }
        for(size_t i=0; i<_values.size(); i++)
        {
            Value const& v = _values[i];
            Value const& v2 = other._values[i];
            if(v.isNull() != v2.isNull() || v.size() != v2.size())
            {
                return false;
            }
            else if (v.isNull() && v.getMissingReason()!=v2.getMissingReason())
            {
                return false;
            }
            else if(memcmp(v.data(), v2.data(), v.size())!=0)
            {
                return false;
            }
        }
        for (size_t i=0; i<_instanceIDs.size(); i++)
        {
            if(_instanceIDs[i]!=other._instanceIDs[i])
            {
                return false;
            }
        }
        for(size_t i=0; i<_valueCoords.size(); i++)
        {
            if(_valueCoords[i]!=other._valueCoords[i])
            {
                return false;
            }
        }
        return true;
    }

    bool operator != (const WindowEdge & other ) const
    {
        return !((*this)==other);
    }

    friend std::ostream& operator<<(std::ostream& stream, const WindowEdge& edge)
    {
        stream<<"{values "<<edge._values.size()<<" coords "<<edge._valueCoords.size()<<" nids "<<edge._instanceIDs.size()<<" following "<<edge._numFollowing<<"}";
        return stream;
    }
};

class ChunkEdge : public unordered_map< Coordinates, shared_ptr<WindowEdge> >
{
    friend std::ostream& operator<<(std::ostream& stream, const ChunkEdge& edge)
    {
        ChunkEdge::const_iterator iter = edge.begin();
        while (iter!= edge.end())
        {
            Coordinates const& coords = iter->first;
            shared_ptr<WindowEdge> const& edge = iter->second;
            stream<<CoordsToStr(coords)<<":"<<(*edge)<<"; ";
            iter++;
        }
        return stream;
    }

public:
    bool operator == (const ChunkEdge & other ) const
    {
        if (size() != other.size() )
        {
            return false;
        }
        ChunkEdge::const_iterator iter = begin();
        ChunkEdge::const_iterator iter2;
        while(iter!=end())
        {
            Coordinates const& coords = iter->first;
            iter2 = other.find(coords);
            if(iter2 == other.end())
            {
                return false;
            }
            shared_ptr<WindowEdge> const& edge = iter->second;
            shared_ptr<WindowEdge> const& other_edge = iter2->second;
            if((edge.get() && !other_edge.get()) || (!edge.get() && other_edge.get()))
            {
                return false;
            }
            if( edge.get() && (*edge) != (*other_edge) )
            {
                return false;
            }
            iter++;
        }
        return true;
    }

    bool operator != (const ChunkEdge & other ) const
    {
        return !((*this) == other);
    }

};

struct VariableWindowMessage
{
    unordered_map<Coordinates, shared_ptr<ChunkEdge> > _chunkEdges;
    unordered_map<Coordinates, shared_ptr< unordered_map <Coordinates, vector<Value> > > > _computedValues;

    void addValues(Coordinates const& chunkPos, Coordinates const& valuePos, vector<Value> const& v)
    {
        shared_ptr< unordered_map<Coordinates, vector<Value> > >& mp = _computedValues[chunkPos];
        if(mp.get()==0)
        {
            mp.reset(new unordered_map <Coordinates, vector<Value> > ());
        }
        assert(mp->count(valuePos)==0);
        (*mp)[valuePos] = v;
    }

    friend std::ostream& operator<<(std::ostream& stream, const VariableWindowMessage& message)
    {
        stream<<"Chunk Edges: "<<message._chunkEdges.size()<<"\n";
        unordered_map<Coordinates, shared_ptr<ChunkEdge> >::const_iterator iter = message._chunkEdges.begin();
        while(iter!= message._chunkEdges.end())
        {
            Coordinates const& coords = iter->first;
            shared_ptr<ChunkEdge> const& edge = iter->second;
            stream<<"   "<<CoordsToStr(coords)<<": "<<(*edge)<<"\n";
            iter++;
        }
        stream<<"Computed Value Chunks: "<<message._computedValues.size()<<"\n";
        unordered_map<Coordinates, shared_ptr< unordered_map <Coordinates, vector<Value> > > >::const_iterator iter2 = message._computedValues.begin();
        while(iter2 != message._computedValues.end())
        {
            Coordinates const& coords = iter2->first;
            stream<<"   "<<CoordsToStr(coords)<<": ";
            shared_ptr< unordered_map <Coordinates, vector<Value> > > const& valChunk = iter2->second;
            unordered_map <Coordinates, vector<Value> >::const_iterator iter3 = valChunk->begin();
            while(iter3!=valChunk->end())
            {
                Coordinates const& coords2 = iter3->first;
                vector<Value> const& vals = iter3->second;
                stream<<CoordsToStr(coords2)<<":{";
                for(size_t j=0; j<vals.size(); j++)
                {
                    stream<<vals[j].size()<<","<<vals[j].getMissingReason()<<" ";
                }
                stream<<"}; ";
                iter3++;
            }
            stream<<"\n";
            iter2++;
        }
        return stream;
    }

    bool hasData() const
    {
        return _chunkEdges.size() > 0 || _computedValues.size() > 0;
    }

    void clear()
    {
        _chunkEdges.clear();
        _computedValues.clear();
    }

    //[nChunkEdges][edgeCoords1][nWindowEdges][windowEdgeCoords1][windowEdge1]..[edgeCoords2]..
    //[nValueChunks][valueChunkCoords1][nValues][valueCoords1][value1]...
    size_t getBinarySize(size_t nDims, size_t nAggs) const
    {
        //nChunkEdges, nValueChunks
        size_t totalSize = 2*sizeof(size_t);
        unordered_map<Coordinates, shared_ptr<ChunkEdge> >::const_iterator iter = _chunkEdges.begin();
        while(iter!=_chunkEdges.end())
        {
            //chunk edge coordinates + nWindowEdges
            totalSize+= nDims*sizeof(Coordinate) + sizeof(size_t);
            shared_ptr<ChunkEdge> const& chunkEdge = iter->second;
            ChunkEdge::iterator innerIter = chunkEdge->begin();
            while(innerIter!=chunkEdge->end())
            {
                //window edge coordinates
                totalSize+= nDims*sizeof(Coordinate);
                shared_ptr<WindowEdge> const& windowEdge = innerIter->second;
                //window edge size
                totalSize+= windowEdge->getBinarySize();
                innerIter++;
            }
            iter++;
        }
        unordered_map<Coordinates, shared_ptr< unordered_map <Coordinates, vector<Value> > > >::const_iterator iter2 = _computedValues.begin();
        while(iter2!=_computedValues.end())
        {
            //value chunk coordinates + nValues
            totalSize+= nDims*sizeof(Coordinate)+ sizeof(size_t);

            shared_ptr <unordered_map<Coordinates, vector<Value> > >const& innerMap = iter2->second;
            unordered_map<Coordinates, vector<Value> >::iterator innerIter = innerMap->begin();
            while(innerIter != innerMap->end())
            {
                //valueCoords + VALSIZE or VALMC
                vector<Value> const& v = innerIter->second;
                assert(v.size() == nAggs);
                totalSize += nDims*sizeof(Coordinate) + sizeof(int64_t)*nAggs;
                for(size_t i =0; i<nAggs; i++)
                {
                    if (!v[i].isNull())
                    {
                        totalSize+=v[i].size();
                    }
                }
                innerIter++;
            }
            iter2++;
        }
        return totalSize;
    }

    bool operator == (const VariableWindowMessage & other) const
    {
        if (_chunkEdges.size() != other._chunkEdges.size() || _computedValues.size() != other._computedValues.size())
        {
            return false;
        }
        unordered_map<Coordinates, shared_ptr<ChunkEdge> >::const_iterator iter = _chunkEdges.begin();
        unordered_map<Coordinates, shared_ptr<ChunkEdge> >::const_iterator oiter;
        while(iter!=_chunkEdges.end())
        {
            Coordinates const& chunkCoords = iter->first;
            oiter = other._chunkEdges.find(chunkCoords);
            if(oiter == other._chunkEdges.end())
            {
                return false;
            }
            shared_ptr<ChunkEdge> const& chunkEdge = iter->second;
            shared_ptr<ChunkEdge> const& oChunkEdge = oiter->second;
            if ((chunkEdge.get() && !oChunkEdge.get()) || (!chunkEdge.get() && oChunkEdge.get()))
            {
                return false;
            }
            if( chunkEdge.get() && (*chunkEdge) != (*oChunkEdge) )
            {
                return false;
            }
            iter++;
        }
        unordered_map<Coordinates, shared_ptr< unordered_map <Coordinates, vector<Value> > > >::const_iterator iter2 = _computedValues.begin();
        unordered_map<Coordinates, shared_ptr< unordered_map <Coordinates, vector<Value> > > >::const_iterator oiter2;
        while(iter2 != _computedValues.end())
        {
            Coordinates const& coords = iter2->first;
            oiter2 = other._computedValues.find(coords);
            if(oiter2 == other._computedValues.end())
            {
                return false;
            }
            shared_ptr< unordered_map<Coordinates, vector<Value> > > const& valMap = iter2->second;
            shared_ptr< unordered_map<Coordinates, vector<Value> > > const& oValMap = oiter2->second;
            if ((valMap.get() && !oValMap.get()) || (!valMap.get() && oValMap.get()) )
            {
                return false;
            }
            if (valMap.get())
            {
                unordered_map<Coordinates, vector<Value> >::const_iterator iter3 = valMap->begin();
                unordered_map<Coordinates, vector<Value> >::const_iterator oiter3;
                while(iter3!=valMap->end())
                {
                    Coordinates const& coords = iter3->first;
                    oiter3 = oValMap->find(coords);
                    if(oiter3 == oValMap->end())
                    {
                        return false;
                    }

                    vector<Value> const& v = iter3->second;
                    vector<Value> const& v2 = oiter3->second;
                    if(v.size()!=v2.size())
                    {
                        return false;
                    }

                    for(size_t i=0; i<v.size(); i++)
                    {
                        if(v[i].isNull() != v2[i].isNull() || v[i].size() != v2[i].size())
                        {
                            return false;
                        }
                        else if (v[i].isNull() && v[i].getMissingReason()!=v2[i].getMissingReason())
                        {
                            return false;
                        }
                        else if(memcmp(v[i].data(), v2[i].data(), v[i].size())!=0)
                        {
                            return false;
                        }
                    }
                    iter3++;
                }
            }
            iter2++;
        }
        return true;
    }

    inline char* marshall (size_t nDims, size_t nAggs, char* buf) const
    {
        size_t* sizePtr = (size_t*) buf;
        *sizePtr = _chunkEdges.size();
        Coordinate* coordPtr = (Coordinate*) (sizePtr+1);

        unordered_map<Coordinates, shared_ptr<ChunkEdge> >::const_iterator iter = _chunkEdges.begin();
        while(iter!=_chunkEdges.end())
        {
            Coordinates const& chunkCoords = iter->first;
            assert(chunkCoords.size() == nDims);
            for(size_t i=0; i<nDims; i++)
            {
                *coordPtr = chunkCoords[i];
                coordPtr++;
            }

            shared_ptr<ChunkEdge> const& chunkEdge = iter->second;
            size_t *edgeSizePtr = (size_t*) coordPtr;
            *edgeSizePtr = chunkEdge->size();
            coordPtr = (Coordinate*) (edgeSizePtr+1);
            ChunkEdge::const_iterator innerIter = chunkEdge->begin();
            while(innerIter!=chunkEdge->end())
            {
                Coordinates const& edgeCoords = innerIter->first;
                assert(edgeCoords.size() == nDims);
                for(size_t i=0; i<nDims; i++)
                {
                    *coordPtr = edgeCoords[i];
                    coordPtr++;
                }
                shared_ptr<WindowEdge>const& windowEdge = innerIter->second;
                coordPtr = (Coordinate*) windowEdge->marshall((char*) coordPtr);
                innerIter++;
            }
            iter++;
        }

        size_t* valuesCountPtr = (size_t*) coordPtr;
        *valuesCountPtr = _computedValues.size();
        coordPtr = (Coordinate*) (valuesCountPtr+1);
        unordered_map<Coordinates, shared_ptr< unordered_map <Coordinates, vector<Value> > > >::const_iterator iter2 = _computedValues.begin();
        while(iter2!=_computedValues.end())
        {
            Coordinates const& chunkCoords = iter2->first;
            assert(chunkCoords.size()==nDims);
            for(size_t i=0; i<nDims; i++)
            {
                *coordPtr = chunkCoords[i];
                coordPtr++;
            }
            size_t* nValuesPtr = (size_t*) coordPtr;
            shared_ptr <unordered_map<Coordinates,vector<Value> > > const& innerMap = iter2->second;
            *nValuesPtr = innerMap->size();
            coordPtr = (Coordinate*) (nValuesPtr+1);
            unordered_map<Coordinates,vector<Value> >::const_iterator innerIter = innerMap->begin();
            while(innerIter != innerMap->end())
            {
                Coordinates const& coords = innerIter->first;
                assert(coords.size()==nDims);
                for(size_t i=0; i<nDims; i++)
                {
                    *coordPtr = coords[i];
                    coordPtr++;
                }
                vector<Value> const& v = innerIter->second;
                assert(v.size() == nAggs);

                for(size_t i =0; i<nAggs; i++)
                {
                    int64_t* sizePtr = (int64_t*) coordPtr;
                    if(v[i].isNull())
                    {
                        *sizePtr = v[i].getMissingReason() * (-1);
                        coordPtr = (Coordinate*) (sizePtr+1);
                    }
                    else
                    {
                        int64_t vsize = v[i].size();
                        *sizePtr = vsize;
                        sizePtr++;
                        char* dataPtr = (char*) sizePtr;
                        memcpy(dataPtr, v[i].data(), vsize);
                        coordPtr = (Coordinate*) (dataPtr+vsize);
                    }
                }
                innerIter++;
            }
            iter2++;
        }
        char* result = (char*) coordPtr;
        assert( (size_t) (result - buf) == getBinarySize(nDims, nAggs) );
        return result;
    }

    inline char* unMarshall (char* data, size_t nDims, size_t nAggs)
    {
        size_t *numEdgesPtr = (size_t*)data;
        size_t numEdges = *numEdgesPtr;
        Coordinate* coordPtr = (Coordinate*) (numEdgesPtr+1);
        for(size_t i=0; i<numEdges; i++)
        {
            Coordinates chunkCoords(nDims);
            for(size_t j=0; j<nDims; j++)
            {
                chunkCoords[j]=*coordPtr;
                coordPtr++;
            }

            shared_ptr<ChunkEdge> &rce = _chunkEdges[chunkCoords];
            if(rce.get()==0)
            {
                rce.reset(new ChunkEdge());
            }
            size_t* numWindowEdgesPtr = (size_t*) (coordPtr);
            size_t numWindowEdges = *numWindowEdgesPtr;
            coordPtr = (Coordinate*) (numWindowEdgesPtr+1);
            for(size_t j=0; j<numWindowEdges; j++)
            {
                Coordinates windowEdgeCoords(nDims);
                for(size_t k=0; k<nDims; k++)
                {
                    windowEdgeCoords[k]=*coordPtr;
                    coordPtr++;
                }
                shared_ptr<WindowEdge> rwe(new WindowEdge());
                coordPtr = (Coordinate*) rwe->unMarshall((char*) coordPtr);
                (*rce)[windowEdgeCoords] = rwe;
            }
        }
        size_t *numValueChunksPtr = (size_t*) coordPtr;
        size_t numValueChunks = *numValueChunksPtr;
        coordPtr = (Coordinate*) (numValueChunksPtr +1);
        for(size_t i=0; i<numValueChunks; i++)
        {
            Coordinates chunkCoords(nDims);
            for (size_t j=0; j<nDims; j++)
            {
                chunkCoords[j]=*coordPtr;
                coordPtr++;
            }
            shared_ptr< unordered_map <Coordinates, vector<Value> > >& valueChunk = _computedValues[chunkCoords];
            if(valueChunk.get()==0)
            {
                valueChunk.reset(new unordered_map<Coordinates, vector<Value> >());
            }
            size_t *numValuesPtr = (size_t*) coordPtr;
            size_t numValues = *numValuesPtr;
            coordPtr = (Coordinate*) (numValuesPtr+1);
            for(size_t j=0; j<numValues; j++)
            {
                Coordinates valueCoords(nDims);
                for (size_t k=0; k<nDims; k++)
                {
                    valueCoords[k] = *coordPtr;
                    coordPtr ++;
                }

                for(size_t k=0; k<nAggs; k++)
                {
                    Value val;
                    int64_t* sizeOrNullPtr = (int64_t*) coordPtr;
                    int64_t valSize = *sizeOrNullPtr;
                    if(valSize <= 0)
                    {
                        val.setNull( (int) (valSize*-1) );
                        coordPtr = (Coordinate*) (sizeOrNullPtr+1);
                    }
                    else
                    {
                        char* dataPtr=(char*) (sizeOrNullPtr+1);
                        val.setData(dataPtr, valSize);
                        coordPtr = (Coordinate*) (dataPtr+valSize);
                    }
                    (*valueChunk)[valueCoords].push_back(val);
                }
            }
            _computedValues[chunkCoords] = valueChunk;
        }
        char* ret = (char*) coordPtr;
        return ret;
    }
};

//how's this for a unit testing framework?
#define CPPUNIT_ASSERT(expr) if (!(expr)) { abort(); }

void testRightEdge()
{
    AggregateLibrary* al = AggregateLibrary::getInstance();
    Type tDouble = TypeLibrary::getType(TID_DOUBLE);
    AggregatePtr sa = al->createAggregate("sum", tDouble);
    CPPUNIT_ASSERT(sa.get() != 0);

    vector<AggregatePtr> sum;
    sum.push_back(sa);

    WindowEdge re;

    Value v1;
    v1.setDouble(1);
    re.addPreceding(v1);
    v1.setNull();
    re.addPreceding(v1);
    v1.setDouble(2);
    re.addPreceding(v1);

    v1.setDouble(3);
    re.addCentral(v1, 0, 0);

    CPPUNIT_ASSERT(re.getNumCoords()==1);
    CPPUNIT_ASSERT(re.getNumValues()==4);

    boost::shared_ptr<AggregatedValue> p = re.churn(3, 0, sum);
    CPPUNIT_ASSERT(p->coord==0 && p->instanceId==0 && p->vals[0].getDouble() == 6);
    CPPUNIT_ASSERT(re.getNumCoords()==0);
    CPPUNIT_ASSERT(re.getNumValues()==3);

    re.addCentral(v1, 1, 2);

    p = re.churn(10, 1, sum);
    CPPUNIT_ASSERT(p->coord==1 && p->instanceId==2 && p->vals[0].getDouble() == 8);
    CPPUNIT_ASSERT(re.getNumCoords()==0);
    CPPUNIT_ASSERT(re.getNumValues()==4);

    v1.setDouble(4);
    re.addCentral(v1, 2, 0);

    v1.setNull();
    re.addCentral(v1, 3, 0);

    size_t size = re.getBinarySize();
    char* buf = new char[size];
    char* end = re.marshall(buf);
    CPPUNIT_ASSERT( (size_t) (end - buf) == size);

    WindowEdge re2;
    re2.unMarshall(buf);
    CPPUNIT_ASSERT(re.getNumCoords() == re2.getNumCoords() );
    CPPUNIT_ASSERT(re.getNumValues() == re2.getNumValues() );

    while(re.getNumCoords())
    {
        boost::shared_ptr<AggregatedValue> p1 = re.churn(4, 1, sum);
        boost::shared_ptr<AggregatedValue> p2 = re2.churn(4, 1, sum);

        CPPUNIT_ASSERT(p1->coord==p2->coord);
        CPPUNIT_ASSERT(p1->instanceId==p2->instanceId);
        CPPUNIT_ASSERT(p1->vals[0]==p2->vals[0]);
    }

    re2.clear();
    CPPUNIT_ASSERT(re2.getNumValues() == 0);

    delete[] buf;

    //simulate 2 preceding + 1 following
    v1.setDouble(1);
    re2.addPreceding(v1);

    v1.setNull();
    re2.addPreceding(v1);

    v1.setDouble(2);
    re2.addCentral(v1, 0, 1);

    v1.setDouble(3);
    re2.addCentral(v1, 1, 0);

    v1.setDouble(4);
    re2.addCentral(v1, 2, 2);

    v1.setDouble(5);
    re2.addFollowing(v1);

    size = re2.getBinarySize();
    buf = new char [size];
    re2.marshall(buf);
    re2.clear();
    re2.unMarshall(buf);

    //1+n+2+3
    p = re2.churn(2, 1, sum);
    CPPUNIT_ASSERT(p->coord==0 && p->instanceId==1 && p->vals[0].getDouble() == 6 && re2.getNumFollowing()==2);

    //n+2+3+4
    p = re2.churn(2, 1, sum);
    CPPUNIT_ASSERT(p->coord==1 && p->instanceId==0 && p->vals[0].getDouble() == 9 && re2.getNumFollowing()==1);

    //2+3+4+5
    p = re2.churn(2, 1, sum);
    CPPUNIT_ASSERT(p->coord==2 && p->instanceId==2 && p->vals[0].getDouble() == 14 && re2.getNumFollowing()==0);

    delete[] buf;
}

void grindAndCompare(VariableWindowMessage const& message, size_t nDims)
{
    size_t binarySize = message.getBinarySize(nDims,1);
    char* buf = new char[binarySize];
    char* buf2 = message.marshall(nDims, 1, buf);
    CPPUNIT_ASSERT( (size_t) (buf2-buf) == binarySize );
    VariableWindowMessage message2;
    message2.unMarshall(buf, nDims, 1);

//        std::cout<<"\nSRC Message: "<<message<<"\n";
//        std::cout<<"\nDST Message: "<<message2<<"\n";

    CPPUNIT_ASSERT(message == message2);
    delete[] buf;
}

void testMessageMarshalling()
{
    VariableWindowMessage message;
    size_t nDims = 3;
    grindAndCompare(message, nDims);

    shared_ptr <ChunkEdge> chunkEdge0 (new ChunkEdge());
    Coordinates coords(nDims);

    coords[0]=0;
    coords[1]=0;
    coords[2]=0;

    Value val;
    val.setDouble(0.0);

    shared_ptr<WindowEdge> windowEdge0 (new WindowEdge());
    windowEdge0->addPreceding(val);
    val.setDouble(0.1);
    windowEdge0->addPreceding(val);
    val.setNull();
    windowEdge0->addCentral(val, 0, 0);
    val.setDouble(0.3);
    windowEdge0->addCentral(val, 1, 0);
    windowEdge0->addCentral(val, 2, 0);
    val.setNull();
    windowEdge0->addFollowing(val);
    (*chunkEdge0)[coords] = windowEdge0;

    coords[1]=1;
    shared_ptr<WindowEdge> windowEdge1 (new WindowEdge());
    val.setDouble(0.5);
    windowEdge1->addPreceding(val);
    (*chunkEdge0)[coords] = windowEdge1;

    coords[1]=0;
    message._chunkEdges[coords]=chunkEdge0;
    grindAndCompare(message, nDims);

    shared_ptr <ChunkEdge> chunkEdge1 (new ChunkEdge());
    shared_ptr<WindowEdge> windowEdge3 (new WindowEdge());
    val.setDouble(0.6);
    windowEdge3->addCentral(val, 3, 0);
    coords[0]=3;
    coords[1]=3;
    coords[2]=4;
    (*chunkEdge1)[coords] = windowEdge3;
    coords[2]=3;
    message._chunkEdges[coords]=chunkEdge1;
    grindAndCompare(message, nDims);

    coords[0]=0;
    coords[1]=0;
    coords[2]=0;
    message._computedValues[coords] = shared_ptr<unordered_map<Coordinates, vector<Value> > > (new unordered_map<Coordinates, vector<Value> >());

    vector<Value> vals(1,val);

    (*message._computedValues[coords])[coords] = vals;
    grindAndCompare(message, nDims);
    Coordinates coords2 = coords;
    coords2[1]=1;
    vals[0].setNull();
    (*message._computedValues[coords])[coords2] = vals;
    grindAndCompare(message, nDims);
    message._computedValues[coords2]= shared_ptr<unordered_map<Coordinates, vector<Value> > > (new unordered_map<Coordinates, vector<Value> >());
    vals[0].setDouble(3.4);
    (*message._computedValues[coords2])[coords2] = vals;
    grindAndCompare(message, nDims);
}

void runVariableWindowUnitTests()
{
    testRightEdge();
    testMessageMarshalling();
}

} //namespace
