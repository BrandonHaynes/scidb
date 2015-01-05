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
#ifndef GROUPER_H
#define GROUPER_H

#include "mysqlwrapper.h"
#include <log4cxx/logger.h>
#include "array/MemArray.h"
#include "array/Metadata.h"
#include "cookgroup.h"
  
using namespace scidb;

class Grouper{
public:
  void loadGroup(const std::vector<ObsPos> &allObs,const std::vector<Image> &allImages,float D2, int T);
  void storeGroup(boost::shared_ptr<MemArray> output);
  int getSize(){return groups.size();};
  int getGroupSize(){return groups.size();};
private:
  std::vector<ObsPos> allObs;
  int allObsCount;
  int binarySearchObsId (int obsid) const;

  boost::shared_ptr<ArrayIterator> _oidIterator;
  boost::shared_ptr<ChunkIterator> _oidChunkit;

  boost::shared_ptr<ArrayIterator> _xIterator;
  boost::shared_ptr<ChunkIterator> _xChunkit;

  boost::shared_ptr<ArrayIterator> _yIterator;
  boost::shared_ptr<ChunkIterator> _yChunkit;

  std::map<int, std::vector<int> > groups;
};

#endif // GROUPER_H
