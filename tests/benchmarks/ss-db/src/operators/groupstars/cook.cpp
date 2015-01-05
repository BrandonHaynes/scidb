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
#include "cook.h"
#include "polygonizer.h"
#include <algorithm>
#include <assert.h>
#include <cmath>

void Cook::cookRawImage() {
  _initialize();
  _provider.onInitialize();
  PixVal pix;
  for (bool remaining = _provider.moveToFirstPixel(); remaining; remaining = _provider.moveToNextPixel()) {
      _provider.getCurrentPixel(pix);
      _checkNext(pix.coord.first, pix.coord.second);
      _ingest(pix);
  }
  _flushFinalizable();  // Post-process and write out.
  _provider.onFinalize();
}

void Cook::_initialize() {
  // Width should be the length of the dimension thatis fastest to
  // traverse (by locality)
  _width = _provider.getImageWidth();
  assert (_width > 0);
  _height = _provider.getImageHeight();
  assert (_height > 0);
  _currentY = 0;

  _lines.resize(_width*2); 
  _current = _lines.begin();
  _previous = _lines.end(); // Force previous to null.
}

void Cook::_checkNext(Idx x, Idx y) {
  assert(x >= 0);
  if(y != _currentY) {
    assert(y == _currentY + 1); // not handling non-consecutive y either.
    _nextLine();
    _currentY = y;
  }
  assert (x < _width); // valid X
}

void Cook::_ingest(PixVal &pix) {
  Idx x = pix.coord.first;
  Idx y = pix.coord.second;
  Val pixVal = pix.val;
  if(pixVal >= _threshold) {
    int objid;
    // continuing or new obj?
    int left = _leftNeighbor(x);
    if(left) {
      int up = _upNeighbor(x);
      if(up) {
        if (left != up) { // Need to merge upper and left objects
          if (left < up) {
            objid = left;
            _mergeObjects(left, up);
          } else {
            objid = up;
            _mergeObjects(up, left);
          }
        } else { // don't need to merge
          objid = left;
        }
      } else {
        objid = left;
      }
    } else { // no left object
      int up = _upNeighbor(x); // optimize this out?
      if(up) {
        _mergeUpper(x); // may merge upper-left and upper-right objs
        objid = up;
      } else {
        objid = _newObjId();
      }
    }
    _current[x] = objid;
    _addPixel(objid, x,y, pixVal);
  }
}


int Cook::_newObjId() {
  return _nextOid++;
}

void Cook::_nextLine() {
    // To save memory, we may want to "finalize" objects once we know
    // all their boundaries.  For now, don't do this.
    
    // Swap buffers and zero-out buffer.
    LineBufferIter temp = _previous;
    if(temp == _lines.end()) temp = _current + _width; // previous line was
    // null.
    _flushFinalizable();
    _previous = _current;
    _current = temp;
    std::fill_n(temp, _width, 0);
}

Oid Cook::_leftNeighbor(Idx x)  {
  if(x > 0)
      return _current[x-1];
  return 0;
}

Oid Cook::_upNeighbor(Idx x) {
  // Check:
  // A B C <-- previous
  // D ?  <-- current 
  // ... need to check A, B, and C.  Will return smallest oid if 
  // multiple possibilities (e.g., A=4, C=5).  Note that oid(C) <
  // oid(A) is possible too. 

  if(_currentY > 0) {
    int current = _previous[x];
    if((x > 0) && (_previous[x-1]) 
       && ((_previous[x-1] < current) || !current)) 
      current = _previous[x-1];
    if((x+1 < _width) && (_previous[x+1]) 
       && ((_previous[x+1] < current) || !current)) 
      current = _previous[x+1];

    return current;
  }
  return 0;

}

void Cook::_mergeUpper(Idx x) {
    // Merge upper pixel objects into a single object.  This only
    // happens when:
    // A 0 B --upper
    // 0 q   -- lower
    // where A, B belong to different objects (currently), and q is
    // qualifying pixel.  (Idx x) refers to the x coordinate of q.
    // 
    // Correct behavior is simple: Find oids of A and B, and merge them 
    // to the dominant object, which we'll define as the one with the
    // lower (earlier) oid.
  
    // check whether we're on bounds:
    if((x < 1) || (x == _width)) {
  return; // Since we're on a boundary-- won't have to do anything.
    }
    int leftoid = _previous[x-1];
    int rightoid = _previous[x+1];
    if (!leftoid || !rightoid) {
  return; // A or B do not belong to objects.
    }
    if (leftoid < rightoid) {
  _mergeObjects(leftoid, rightoid);
    } else if(rightoid < leftoid) {
  _mergeObjects(rightoid, leftoid);
    }
  
}


void Cook::_mergeObjects(int majoroid, int minoroid) {
  // Put pixels of the minor oid object into the major oid object.
  // Then fix the two-line buffer so that the pixels belonging to the
  // minor object get patched with the major object's id.
  if(majoroid == minoroid) return; // nothing to do.

  Observ& minor = _obs[minoroid];
  Observ& major = _obs[majoroid];

  // Copy minor's pixels to major's.
  major.pixels.insert(major.pixels.end(), 
         minor.pixels.begin(), minor.pixels.end());
  // Patch the pixels in the minor that occur in our buffer (lines
  // indexed _currentY and _currentY-1).
  for_each(minor.pixels.begin(), minor.pixels.end(), 
     _obsSet(majoroid, 
      std::pair<Idx,LineBufferIter>(_currentY, _current), 
      std::pair<Idx,LineBufferIter>(_currentY-1, _previous)));
  
  // Remove the minor object.
  _obs.erase(minoroid);
  // Obj scratch();
  // remove_copy_if(minor.second.begin(), minor.second.end(), 
  //     back_inserter(scratch),
  //     bind2nd(less<int>(),_currentY-1));
  // We could do this without the scratch vector.  Optimize this
  // later.
  
}

void Cook::_addPixel(Oid obsid, Idx x, Idx y, Val pixval) {
    _updateFinalizable(obsid);
    // Get obj
    _obs[obsid].pixels.push_back(PixVal(x,y, pixval));
    //std::cout << obsid << " adding " << x << "," << y << std::endl;
}


void Cook::_computePoly(const PixVect& p, CoordVect& vertices) {
  Polygonizer<int> pol;
  // Adapt pixel values to a plain coordinate vector.
  std::vector<Coord> cv(p.size());
  int cursor=0;

  for(PixVect::const_iterator i = p.begin(); i != p.end(); i++) {
    cv[cursor] = i->coord;
    cursor++;
  }
  pol.findPoly(cv, vertices);
}

void Cook::_updateFinalizable(Oid o) {
    _fresh[o] = 1;
    _finalizable.erase(o);
}

void Cook::_flushFinalizable() {
  // Apply postprocessing.
  _obsPostProc opp;
  for_each(_finalizable.begin(), _finalizable.end(), 
      _applyMap<_obsPostProc>(_obs, opp));
  // Write out.
  _obsWrite obsWrite (_provider);
  for_each(_finalizable.begin(), _finalizable.end(), 
     _applyMap<_obsWrite>(_obs, obsWrite));
  // Stop tracking
  for(OidIter i = _finalizable.begin(); i != _finalizable.end(); ++i) {
    _obs.erase(i->first);
  }
  // Reset Oid list.
  _finalizable = _fresh;
  _fresh.clear();
}


void Cook::_obsPostProc::operator()(std::pair<const Oid,Observ>& p) {
  Observ& o = p.second;

  // Compute props
  Float pixSum = 0;
  Float weightSumX = 0;
  Float weightSumY = 0;
  Float cX;
  Float cY;

  for(PixVect::iterator i = o.pixels.begin(); i != o.pixels.end(); i++) {
    pixSum += static_cast<Float>(i->val);
    weightSumX += static_cast<Float>(i->val) * static_cast<Float>(i->coord.first);
    weightSumY += static_cast<Float>(i->val) * static_cast<Float>(i->coord.second);
  }
  o.centroidX = cX = weightSumX / pixSum;
  o.centroidY = cY = weightSumY / pixSum;
  o.pixelSum = pixSum;

  Float distSum = 0;
  Float dist;
  Float distX;
  Float distY;
  for(PixVect::iterator i = o.pixels.begin(); i != o.pixels.end(); i++) {
    distX = static_cast<Float>(i->coord.first) - cX;
    distY = static_cast<Float>(i->coord.second) - cY;
    dist =  sqrt(distX*distX + distY*distY);
    distSum += static_cast<Float>(i->val) * dist;
  }
  o.averageDist = distSum / pixSum;

  // Compute polygon edges.
  _computePoly(o.pixels, o.polygons);
}
void Cook::_obsWrite::operator()(std::pair<const Oid,Observ>& p) {
  Observ& o = p.second;
  o.observId = p.first;
  _provider.onNewObservation(o);
}

template <typename T>
Cook::_applyMap<T>::_applyMap(ObsMap& om, T& f) 
    : map(om), functor(f) {}

template <typename T>
void Cook::_applyMap<T>::operator() (std::pair<const Oid, int>& p) {
  ObsMap::iterator valIt = map.find(p.first);
  if(valIt != map.end()) {
    functor(*valIt);
  }
}
