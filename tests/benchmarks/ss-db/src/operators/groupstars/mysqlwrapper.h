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
#ifndef MYSQLWRAPPEr_H
#define MYSQLWRAPPEr_H

#include <list>
#include <set>
#include <map>
#include <sstream>
#include <vector>
#include <utility>
#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
using namespace log4cxx;

void appendInClause (std::stringstream &str, const std::set<int> &values);
void appendInClause (std::stringstream &str, const std::vector<int> &values);

// for CSV bulk loading
std::string getCurDir(LoggerPtr logger);
int prepareCsvFile(const std::string &tablename, LoggerPtr logger);
int prepareCsvFileFullPath(const std::string &csvname, LoggerPtr logger);
void closeCsvFile(int fd, LoggerPtr logger);

class CsvBuffer {
public:
  CsvBuffer (LoggerPtr logger, const std::string &tablename, size_t bufsize);
  CsvBuffer (LoggerPtr logger, const std::string &tablename, size_t bufsize, const std::string &csvFullpath);
  ~CsvBuffer ();
  // variable arguments functions can't be wrapped. so, this is not write(), but written()
  void written (int sprintfRet);
  void close ();
  void loadToMysql ();
  char * curbuf () const { return _buf + _bufused; }

  LoggerPtr _logger;
  std::string _tablename;
  std::string _pathbuf;
  std::string _csvname;
  int _bufsize;
  int _bufused;
  char *_buf;
  int _fd;
};

// container for pixels. this is cheap to pass by value.
struct ImagePixels {
  int externalWidth, externalHeight; // apparent size
  int internalWidth, internalHeight; // could be smaller than external
  int internalXOffset, internalYOffset;
  int32_t* internalArray; // raw pointer. fast to use
  boost::shared_array<int32_t> internalArrayAutoPtr; // to automatically revoke internalArray
  inline int32_t getPixel (int x, int y) const {
    return internalArray[(y - internalYOffset) * internalWidth +  x - internalXOffset];
  }
  inline void setPixel (int x, int y, int32_t pix) {
    assert (y - internalYOffset >= 0);
    assert (y - internalYOffset < internalHeight);
    assert (x - internalXOffset >= 0);
    assert (x - internalXOffset < internalWidth);
    internalArray[(y - internalYOffset) * internalWidth +  x - internalXOffset] = pix;
  }
};

struct Image {
  Image () {};
  Image (int32_t _imageid,int32_t _xstart,int32_t _ystart,int32_t _xend,int32_t _yend, int32_t _time,int32_t _cycle);
  int32_t imageid;
  int32_t xstart;
  int32_t ystart;
  int32_t xend;
  int32_t yend;
  int32_t time;
  int32_t cycle;
  std::string tablename;
  std::string toString() const {
    std::stringstream str;
    str << "imageid=" << imageid << ", xstart=" << xstart << ", ystart=" << ystart << ", xend=" << xend
        << ", yend=" << yend << ", time=" << time << ", cycle=" << cycle << ", tablename=" << tablename;
    return str.str();
  }
  int32_t getWidth () const { return xend - xstart; }
  int32_t getHeight () const { return yend - ystart; }
  //ImagePixels getPixels(MYSQL *mysql, LoggerPtr logger, const std::set<int> &tiles) const;
};

// x-y lengthes for one tile
// tile is a chunk in Raw_xxx
#define TILE_SIZE 100
#define TILE_X_Y_RATIO 1000

struct Point {
  Point() : x(0), y(0) {}
  Point(int32_t xArg, int32_t yArg) : x(xArg), y(yArg) {}
  int32_t x, y;
};
struct Rect {
  int32_t xstart, ystart, xend, yend;
};

struct Observ; // defined in cook.h
// Represents a star.
struct Observation {
  Observation() {};
  //Observation(const MYSQL_ROW &row);
  //static void newObserv(Observ &observ, const MYSQL_ROW &row);
  Observation(const Observ &observ, const Image &image);
  int32_t obsid; // unique id for observation. start from 1
  int32_t imageid;
  float averageDist;
  long long pixelSum;
  int32_t time, cycle; // denormalized data for convenience. can be retrieved from imageid.
  Point center; // location of this observation defined as the center of polygons. in world coordinate.
  Rect bbox; // bounding box of polygons. in world coordinate.
  std::vector<Point> polygons; // polygons that define this observation in world coordinate. This is empty when constructed by MYSQL_ROW. (need to complement in separate method)
};
#endif // MYSQLWRAPPEr
