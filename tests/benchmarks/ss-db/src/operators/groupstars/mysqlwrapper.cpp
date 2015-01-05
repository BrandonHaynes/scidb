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
#include "mysqlwrapper.h"
#include <cmath>
#include <iostream>
#include <sstream>
#include <string.h>
#include "cook.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <cstdio>
#include <cstdlib>

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif

using namespace std;

void appendInClause (std::stringstream &str, const std::set<int> &values) {
  std::vector<int> vec;
  vec.insert (vec.end(), values.begin(), values.end());
  appendInClause (str, vec);
}
void appendInClause (std::stringstream &str, const std::vector<int> &values) {
  assert (values.size() > 0);
  str << " (" << values[0];
  for (size_t i = 1; i < values.size(); ++i) {
    str << "," << values[i];
  }
  str << ")";
}


std::string getCurDir(LoggerPtr logger) {
  char pathbuf[512];
  char *res = ::getcwd(pathbuf, 512);
  if (res == NULL) {
    LOG4CXX_ERROR(logger, "failed to getcwd()");
    throw std::exception();
  }
  return pathbuf;
}
int prepareCsvFile(const std::string &tablename, LoggerPtr logger) {
  string pathbuf = getCurDir(logger);
  LOG4CXX_INFO(logger, "current dir=" << pathbuf);
  string csvname = pathbuf + "/" + tablename + ".csv";
  return prepareCsvFileFullPath (csvname, logger);
}
int prepareCsvFileFullPath(const std::string &csvname, LoggerPtr logger) {
  if (std::remove(csvname.c_str()) == 0) {
    LOG4CXX_INFO(logger, "deleted existing file " << csvname << ".");
  }
  int fd = ::open (csvname.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    LOG4CXX_ERROR(logger, "could not create csv file " << csvname << ". errno=" << errno);
  }
  return fd;
}
void closeCsvFile(int fd, LoggerPtr logger) {
  int fsyncRet = ::fsync (fd);
  if (fsyncRet != 0) {
    LOG4CXX_ERROR(logger, "error on fsync a temp file. errno=" << errno);
  }
  int ret = ::close(fd);
  if (ret != 0) {
    LOG4CXX_ERROR(logger, "error on closing a temp file. errno=" << errno);
  }
}

Image::Image (int32_t _imageid,int32_t _xstart,int32_t _ystart,int32_t _xend,int32_t _yend, int32_t _time,int32_t _cycle) {
  imageid = _imageid;
  xstart = _xstart;
  ystart = _ystart;
  xend = _xend;
  yend = _yend;
  time = _time;
  cycle = _cycle;
}

Observation::Observation(const Observ &observ, const Image &image) {
  obsid = observ.observId;
  imageid = image.imageid;
  time = image.time;
  cycle = image.cycle;
  averageDist = observ.averageDist;
  pixelSum = observ.pixelSum;
  center.x = observ.centroidX + image.xstart;
  center.y = observ.centroidY + image.ystart;
  bbox.xstart = 1 << 30;
  bbox.ystart = 1 << 30;
  bbox.xend = -1;
  bbox.yend = -1;
  for (size_t i = 0; i < observ.polygons.size(); ++i) {
    int32_t x = observ.polygons[i].first + image.xstart;
    int32_t y = observ.polygons[i].second + image.ystart;
    bbox.xstart = min (bbox.xstart, x);
    bbox.ystart = min (bbox.ystart, y);
    bbox.xend = max (bbox.xend, x);
    bbox.yend = max (bbox.yend, y);
    polygons.push_back(Point(x, y));
  }
}

CsvBuffer::CsvBuffer (LoggerPtr logger, const std::string &tablename, size_t bufsize) :  _logger(logger), _tablename (tablename), _bufsize (bufsize), _bufused(0), _buf (new char[bufsize]) {
  _fd = prepareCsvFile(_tablename, _logger);
  _pathbuf = getCurDir(_logger);
  _csvname = _pathbuf + "/" + _tablename + ".csv";
}
CsvBuffer::CsvBuffer (LoggerPtr logger, const std::string &tablename, size_t bufsize, const std::string &csvFullpath) :  _logger(logger), _tablename (tablename), _bufsize (bufsize), _bufused(0), _buf (new char[bufsize]) {
  _fd = prepareCsvFileFullPath(csvFullpath, _logger);
  _pathbuf = getCurDir(_logger);
  _csvname = csvFullpath;
}

CsvBuffer::~CsvBuffer () {
  delete[] _buf;
  _buf = NULL;
}

void CsvBuffer::written (int sprintfRet) {
  if (sprintfRet <= 0) {
    LOG4CXX_ERROR(_logger, "failed to sprintf");
    throw new std::exception ();
  }
  _bufused += sprintfRet;
  if (_bufused > _bufsize * 9 / 10) {
    int writeRet = ::write (_fd, _buf, _bufused);
    if (writeRet == -1) {
      LOG4CXX_ERROR(_logger, "error on writing a temp file. errno=" << errno);
    }
    _bufused = 0;
  }
}
void CsvBuffer::close () {
  if (_bufused > 0) {
    int writeRet = ::write (_fd, _buf, _bufused);
    if (writeRet == -1) {
      LOG4CXX_ERROR(_logger, "error on writing a temp file. errno=" << errno);
    }
    _bufused = 0;
  }
  closeCsvFile(_fd, _logger);
  _fd = 0;
}
