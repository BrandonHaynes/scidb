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

#ifndef PIXELPROVIDER_H
#define PIXELPROVIDER_H

#include "array/Metadata.h"
#include "array/MemArray.h"
#include "Cook.h"

using namespace scidb;

#define OID_ID 0
#define CENTER_ID 1
#define POLYGON_ID 2
#define SUMPIX_ID 3
#define AVGDIST_ID 4
#define POINT_ID 5

#define ITER_MODE ConstChunkIterator::IGNORE_EMPTY_CELLS | ConstChunkIterator::IGNORE_NULL_VALUES

class  ImageProvider: public PixelProvider
{
public:
ImageProvider(boost::shared_ptr<ConstArrayIterator> input, boost::shared_ptr<Array> output, AttributeID aid) ;

  int getImageWidth();
  int getImageHeight();

  bool onNewChunk;
 
  void getCurrentPixel(PixVal&/*out*/) ;

  bool moveToFirstPixel();

  bool moveToNextPixel();

  void onInitialize();

  void onNewObservation(Observ &obs);

  void onFinalize();
 
  bool nextArray();

  bool hasNextArray();

  void initializeOutput(Coordinates const& pos);
  int getId(){return _imageId;};
private:
  static const int xIdx=2;
  static const int yIdx=1;
  static const int zIdx=0;
  int _imageId;
  int _observLine;
  AttributeID _aid;
  boost::shared_ptr<Array> _inputArray;
  boost::shared_ptr<ConstArrayIterator> _arrayIterator;
  boost::shared_ptr<ConstChunkIterator> _chunkIterator;

  boost::shared_ptr<Array> _outputArray; 
  
  boost::shared_ptr<ArrayIterator> _oidIterator;
  boost::shared_ptr<ArrayIterator> _centerIterator;
  boost::shared_ptr<ArrayIterator> _polygonIterator ;
  boost::shared_ptr<ArrayIterator> _sumpixIterator ;
  boost::shared_ptr<ArrayIterator> _avgdistIterator ;
  boost::shared_ptr<ArrayIterator> _pointIterator ;

  boost::shared_ptr<ChunkIterator> _oidCItr;
  boost::shared_ptr<ChunkIterator> _centerCItr;
  boost::shared_ptr<ChunkIterator> _polygonCItr;
  boost::shared_ptr<ChunkIterator> _sumpixCItr;
  boost::shared_ptr<ChunkIterator> _avgdistCItr;
  boost::shared_ptr<ChunkIterator> _pointCItr;

  Value _currentItem;
  Coordinates _currentPos;

  inline void _saveItem(int x, int y);

};

#endif

