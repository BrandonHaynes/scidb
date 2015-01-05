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
#ifndef COOK_H
#define COOK_H

#include <vector>
#include <map>
#include <utility>
#include <list>
// Cooking functions to convert raw image data to
// observation data.


typedef int Idx;
typedef int Val;
typedef std::pair<Idx,Idx> Coord;
typedef std::vector<Coord> CoordVect;
struct PixVal {
  PixVal() : coord(0, 0), val(0) {};
  PixVal(Idx xArg, Idx yArg, Val valArg) : coord(xArg, yArg), val(valArg) {};
  PixVal(const Coord &coordArg, Val valArg) : coord(coordArg), val(valArg) {};
  Coord coord;
  Val val;
};
typedef std::vector<PixVal> PixVect;
typedef int Oid;
typedef int GroupID;
typedef double Float;
typedef std::vector<Oid>::iterator LineBufferIter;
struct Observ {
  Oid observId; // unique id for observation. start from 1
  int centroidX, centroidY;
  int  boxxstart, boxystart;
  int boxxend, boxyend;
  double averageDist;
  long long pixelSum;
  PixVect pixels;
  CoordVect polygons;
  int time;
  int imageId;
};
typedef std::list<Observ> ListofObs;
typedef std::map<int, std::pair<int, int> > imageCoordinates;



// callback interface for cooking
class PixelProvider {
public:
  virtual int getImageWidth() = 0;
  virtual int getImageHeight() = 0;

  // this should return positions starting from zero.
  virtual void getCurrentPixel(PixVal&/*out*/) = 0;

  // move cursor to the most left top (x=0, y=0)
  // return false if no more pixel is available, true otherwise.
  virtual bool moveToFirstPixel() = 0;

  // move cursor to right (x=x+1. if x==_width, x=0 and y=y+1)
  // NOTE: if your underlying storage doesn't provide this order,
  // buffer the data in your Provider to provide in this order.
  // return false if no more pixel is available, true otherwise.
  virtual bool moveToNextPixel() = 0;

  // called back on initialize
  virtual void onInitialize() = 0;

  // called back when a new observation is found (finalized)
  virtual void onNewObservation(Observ &obs) = 0;

  // called when all processing is done
  virtual void onFinalize() = 0;
};

class Cook {
public:
  // PixeProvider should be your own class.
  Cook (PixelProvider &provider, int threshold) : _provider(provider), _threshold(threshold) {};

  // call this function to cook raw image data.
  void cookRawImage (); 
  Oid getNextOid() const { return _nextOid;}
  void setNextOid(Oid oid) { _nextOid = oid;}

private:
  PixelProvider &_provider;
  int _threshold;
  Oid _nextOid;

  void _initialize();
  void _checkNext(Idx x, Idx y);
  void _nextLine();
  void _ingest(PixVal &pix);

  Oid _leftNeighbor(Idx x);
  Oid _upNeighbor(Idx x);
  void _mergeUpper(Idx x);
  void _mergeObjects(int majoroid, int minoroid);
  void _addPixel(Oid obsid, Idx x, Idx y, Val pixval);
  void _updateFinalizable(Oid o);
  void _flushFinalizable();

  static void _computePoly(const PixVect& p, CoordVect& vertices);

  int _newObjId();

  /// Updates our pixel buffer for object merges.
  struct _obsSet : public std::unary_function<PixVal, void> {
    _obsSet(int oid_, 
      std::pair<Idx,LineBufferIter> one_,  // line one (previous)
      std::pair<Idx,LineBufferIter> two_) : // line two (current)
        one(one_), two(two_), oid(oid_) {}
    void operator() (PixVal& p) {
      if(p.coord.second  == one.first) one.second[p.coord.first] = oid;
      if(p.coord.second == two.first) two.second[p.coord.first] = oid;
    }
    std::pair<Idx,LineBufferIter> one;
    std::pair<Idx,LineBufferIter> two;
    int oid;
  };
  typedef std::unary_function<std::pair<const Oid,Observ>, void> ObsFunctor;
  struct _obsPostProc : public ObsFunctor {
    _obsPostProc() {}
    void operator() (std::pair<const Oid,Observ>& p);
  };
  struct _obsWrite : public ObsFunctor {
    _obsWrite(PixelProvider &provider) : _provider(provider) {}
    void operator() (std::pair<const Oid,Observ>& p);
    PixelProvider &_provider;
  };
  typedef std::unary_function<std::pair<Oid, int>, void> OidFunctor;
  typedef std::map<Oid,Observ> ObsMap;
  template<typename T>
  struct _applyMap : public OidFunctor {
    _applyMap(ObsMap& om, T& f);
    void operator() (std::pair<const Oid, int>& p);
    ObsMap& map;
    T& functor;
  };

  int _width;
  int _height;
  int _currentY;

  std::vector<Oid> _lines; // 2-line buffer of observ ids
  LineBufferIter _current; // Ptr to current line
  LineBufferIter _previous; // Ptr to previous line

  ObsMap _obs; // in-memory buffer of observations

  typedef std::map<Oid, int>::const_iterator OidIter;
  std::map<Oid, int> _finalizable; // Oids that may be finalized.
  std::map<Oid, int> _fresh; // Oids detected this line.
};
#endif // COOK_H
