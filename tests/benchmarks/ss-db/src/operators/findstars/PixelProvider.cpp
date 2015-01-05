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

#include "PixelProvider.h"


ImageProvider::ImageProvider(boost::shared_ptr<ConstArrayIterator> aItr, boost::shared_ptr<Array> output, AttributeID aid):
    _observLine(0), 
    _aid(aid), 
    _arrayIterator(aItr),
    _outputArray(output),
    _oidIterator(output->getIterator(OID_ID)),
    _centerIterator(output->getIterator(CENTER_ID)),
    _polygonIterator(output->getIterator(POLYGON_ID)),
    _sumpixIterator(output->getIterator(SUMPIX_ID)),
    _avgdistIterator(output->getIterator(AVGDIST_ID)),
    _pointIterator(output->getIterator(POINT_ID))
{
}

inline void initializeOutput( boost::shared_ptr<ArrayIterator>& stateArrayIterator,
                              boost::shared_ptr<ChunkIterator>& stateChunkIterator,
                              Coordinates const& outPos)
{
  boost::shared_ptr<Query> query(stateArrayIterator->getQuery());
  Chunk& stateChunk = stateArrayIterator->newChunk(outPos);
  //cout << "\t newChunk() "<< outPos;
  stateChunk.setSparse(true);
  stateChunkIterator = stateChunk.getIterator(query);
  //cout << "\t getItrator()" << endl; flush(cout);
}
inline void setOutputPosition(  boost::shared_ptr<ArrayIterator>& stateArrayIterator,
                                boost::shared_ptr<ChunkIterator>& stateChunkIterator,
                                Coordinates const& outPos)
{
  if (!stateChunkIterator || !stateChunkIterator->setPosition(outPos))
  {
     //cout << "ok I couldn't set the pos" << endl; flush(cout);
     if (stateChunkIterator) {
         stateChunkIterator->flush();
         stateChunkIterator.reset();
     }
     //cout << "flushed the CiT" << endl; flush(cout);

     if (!stateArrayIterator->setPosition(outPos))
     {
       //cout << "Even the AiT couldn't set .. do it" << endl; flush(cout);
       initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
       //cout << "Done" << endl; flush(cout); 
     }
     else
     {
       //cout << "The AiT was able .. Updating chunk .. or I dunno" << endl; flush(cout);
       boost::shared_ptr<Query> query(stateArrayIterator->getQuery());
       Chunk& stateChunk = stateArrayIterator->updateChunk();
       stateChunkIterator = stateChunk.getIterator(query, ChunkIterator::APPEND_CHUNK);
       //cout << "Done" << endl; flush(cout);
     }
     if (!stateChunkIterator->setPosition(outPos))
         throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
  }
}
int ImageProvider::getImageWidth() 
{
  return _arrayIterator->getChunk().getArrayDesc().getDimensions()[yIdx].getLength(); 
}

int ImageProvider::getImageHeight() 
{
  return _arrayIterator->getChunk().getArrayDesc().getDimensions()[xIdx].getLength(); 
}

void ImageProvider::getCurrentPixel(PixVal &pix)
{
//     Value item = _chunkIterator->getItem();
    pix.val = _currentItem.getInt32();
    pix.coord.first = _currentPos[xIdx];
    pix.coord.second = _currentPos[yIdx];

}

bool ImageProvider::moveToFirstPixel() 
{
  //cout <<" moving to the next image ";flush(cout);  
  _chunkIterator= _arrayIterator->getChunk().getConstIterator(ITER_MODE);
  //cout<< " .. chunk";flush(cout);
  //if(!_chunkIterator->setPosition(pos)) 
  //{
   // cout << " .. Error in chunks"<< endl;flush(cout);
  //}
  //cout << " Going to next image : "<< _chunkIterator->getPosition() << endl;flush(cout); 
  if (!_chunkIterator->end()) {
    _currentItem = _chunkIterator->getItem();
    _currentPos = _chunkIterator->getPosition();
    ++(*_chunkIterator);
    //cout << " .. Done"<< endl;flush(cout);
    return true;
  } else {
    //cout << ".. Early chunk end .. Error"<< endl;flush(cout);
    return false;
  }
}

bool ImageProvider::moveToNextPixel()
{
  // FIXME: we only support image level chunking
  
  if(onNewChunk){onNewChunk= false; return false;} 
  if(_chunkIterator->end()) return false;  
  if(_chunkIterator->getPosition()[zIdx] != _imageId) return false;
  _currentItem = _chunkIterator->getItem();
  _currentPos = _chunkIterator->getPosition();
  ++(*_chunkIterator);
  if(_chunkIterator->end())
  {
    onNewChunk=true;
  }
 return true;
}

void ImageProvider::onInitialize()
{
    //FIXME: We only support image level chunking
    //cout << "getting chunk" << endl;
    _chunkIterator=_arrayIterator->getChunk().getConstIterator(ITER_MODE);
    _imageId= _arrayIterator->getPosition()[zIdx];
    onNewChunk= false;
}

void ImageProvider::onNewObservation(Observ &obs) 
{
  //TODO write observation
  Value item(TypeLibrary::getType(TID_INT64));
  //DEBUG
  //Coordinates pos = _oidCItr->getPosition();
  PixVect pixels = obs.pixels;
  CoordVect polygons = obs.polygons;

  item.setInt64(obs.observId);
  Value item2(TypeLibrary::getType(TID_BOOL));
  item2.setBool(true);
 
  Coordinates pos(3);
  pos[zIdx] = _imageId;
  for(vector<PixVal>::iterator pixel = pixels.begin(); pixel != pixels.end(); ++pixel) 
  {
    pos[yIdx] = pixel->coord.second;
    pos[xIdx] = pixel->coord.first;
    //cout << pos[0] << "," << pos[1] << "," << pos[2] << endl; flush(cout);
    // Mark the point oid
    setOutputPosition(_oidIterator,_oidCItr,pos);
    //cout << "position was set" << endl; flush(cout);
    _oidCItr->writeItem(item);
    // Write out all the Points related to the observation
    setOutputPosition(_pointIterator,_pointCItr, pos);
    _pointCItr->writeItem(item2);
  }   
  //cout << " wrote the points " << endl; flush(cout);
  // Now move to the center
  pos[yIdx] = obs.centroidY;
  pos[xIdx] = obs.centroidX;
   
  // mark the center's oid
  setOutputPosition(_oidIterator, _oidCItr, pos);
  _oidCItr->writeItem(item);
  
  // center
  setOutputPosition(_centerIterator, _centerCItr, pos);
  _centerCItr->writeItem(item2);
 
  // sumpix
  Value item4(TypeLibrary::getType(TID_INT64));
  item4.setInt64(obs.pixelSum);
  setOutputPosition(_sumpixIterator, _sumpixCItr, pos);
  _sumpixCItr->writeItem(item4);

  // Avg Distributtion
  Value item5(TypeLibrary::getType(TID_DOUBLE));
  item5.setDouble(obs.averageDist);
  setOutputPosition(_avgdistIterator, _avgdistCItr, pos);
  _avgdistCItr->writeItem(item5);

  // Polygon's point
  // Oid is marked already
  Value item6(TypeLibrary::getType(TID_INT32));
  int i=1;
  for (vector<pair<Idx,Idx> >::iterator polygon =polygons.begin();  polygon != polygons.end(); ++polygon) 
  {
    pos[yIdx] = polygon->second;
    pos[xIdx] = polygon->first;
    setOutputPosition(_polygonIterator, _polygonCItr, pos);
    item6.setInt32(i++);
    _polygonCItr->writeItem(item6);
  }
}
bool ImageProvider::nextArray()
{
 ++(*_arrayIterator);
 return !_arrayIterator->end();
}
bool ImageProvider::hasNextArray()
{
 return !_arrayIterator->end();
}
void ImageProvider::onFinalize()
{
   if (_oidCItr) _oidCItr->flush();
   if (_pointCItr) _pointCItr->flush();
   if (_centerCItr) _centerCItr->flush();
   if (_sumpixCItr) _sumpixCItr->flush();
   if (_avgdistCItr) _avgdistCItr->flush();
   if (_polygonCItr) _polygonCItr->flush(); 
}
