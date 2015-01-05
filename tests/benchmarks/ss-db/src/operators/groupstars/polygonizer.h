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

#ifndef POLYGONIZER_H
#define POLYGONIZER_H

#include <iostream>
#include <assert.h>

// Declaration of Polygonizer class
// Questions? Contact Daniel Wang
template <typename C>
class Polygonizer {
public:
    
    typedef std::pair<C,C> Coord;
    typedef std::vector<Coord> Obj;
    typedef std::vector<Coord> CoordVect;

    Polygonizer();
    ~Polygonizer();

    void findPoly(Obj const& o, CoordVect& vertices);    
    void findAdj(const Obj);

  
private:
  struct lessCoord {
    bool operator()(const Coord& l, const Coord& r) const {
      return (l.first < r.first)  
  || ((l.first == r.first) && (l.second < r.second));
    }
  };
  typedef std::map<Coord,bool,lessCoord> PMap;

  struct FirstPicker {
    FirstPicker(Coord& c) : best(c) {}

    void operator() (const Coord& c) {
      if((c.first < best.first) 
   || ((c.first == best.first) && (c.second < best.second)))  {
  best = c;
      }
    }
    Coord& best; // for_each passes by value, so we must use a reference.
  };
  struct MapInserter {
    MapInserter(PMap& p) : pmap(p) { }
    void operator() (const Coord& c) {
      pmap.insert(typename PMap::value_type(c,1));
    }
    PMap& pmap;
  };

  const Coord& chooseNext(const Coord& loc, const Coord& dir, const PMap& p, Coord& nextDir);

  void subtract(Coord& result, const Coord& op1, const Coord& op2) {
    result.first = op1.first - op2.first;
    result.second = op1.second - op2.second;
  }
  void add(Coord& result, const Coord& op1, const Coord& op2) {
    result.first = op1.first + op2.first;
    result.second = op1.second + op2.second;
  }


  Coord* _directions;

};


template <typename C>
Polygonizer<C>::Polygonizer() 
    : _directions(new Coord[17]) // for curve-tracing.
{
    // Lookup table to make the direction logic simpler
    Coord* d = _directions; // cast away const to build.
    d[0] = Coord(0,-1);
    d[1] = Coord(1,-1);
    d[2] = Coord(1,0);
    d[3] = Coord(1,1);
    d[4] = Coord(0,1);
    d[5] = Coord(-1,1);
    d[6] = Coord(-1,0);
    d[7] = Coord(-1,-1);
    d[8] = Coord(0,-1);
    d[9] = Coord(1,-1);
    d[10] = Coord(1,0);
    d[11] = Coord(1,1);
    d[12] = Coord(0,1);
    d[13] = Coord(-1,1);
    d[14] = Coord(-1,0);
    d[15] = Coord(-1,-1);
    d[16] = Coord(0,0); // Safety value.
}

template <typename C>
Polygonizer<C>::~Polygonizer() {
    delete[] _directions;
}



template <typename C>
void Polygonizer<C>::findPoly(const Obj& o, CoordVect& vertices) {
    // Idea #3:
    // Curve-trace.  I think this is workable.
    // Start from arbitrary known point and vector.  We'll pick the
    // top-most and left-most pixel.  Trace the object edge clockwise
    // until we return to the starting point.
  
    // Step 1: put pixels into hash table. O(n)
    PMap pixels;
    for_each(o.begin(), o.end(), MapInserter(pixels)); // build map
  
    // Step 2: Find left-most, top-most pixel. O(n)
    Coord current = o[0];
    FirstPicker f(current);
    for_each(o.begin(), o.end(), f);
    Coord initpos = current;
    Coord dir;
    Coord dirNext;
    Coord next;
    // Initial direction: Upwards and left
    dir.first = -1;
    dir.second = -1;
  
    Coord initDir;
    vertices.resize(0);
  
    //  std::cout << "Finding poly from " <<  o.size() << " pixels" << "in map " << pixels.size() << std::endl;
    // Prime next pixel
    next = chooseNext(current, dir, pixels, dirNext);
    initDir = dirNext;
    dir = dirNext;

    do {
  // Add point to polygon.
  vertices.push_back(current);

  current = next;
  // Choose next pixel
  next = chooseNext(current, dir, pixels, dirNext);
  if(vertices.size() > (2 + 2*o.size())) {
      std::cout << "Oversize for object. Bailing out" << std::endl;
      break;
  }
  dir = dirNext;

    } while((current != initpos) || (dir != initDir));  
  
}

template <typename C>
std::pair<C,C> const& Polygonizer<C>::chooseNext(const Polygonizer<C>::Coord& loc,
             const Polygonizer<C>::Coord& dir, 
             const PMap& p, 
             Polygonizer<C>::Coord& nextDir) {
    int index;
    int leftTurn = 1;
    Coord prosp;
    typename PMap::const_iterator piter;

    if(p.size() == 1) 
  return loc; // There is no next.

    // Compensate for diagonals: okay to turn 90 degrees left for them.
    if(2 == (dir.first*dir.first) + (dir.second*dir.second)) { // diagonal?
  leftTurn = 2;
    }
    // Seek to current direction.
    for(index=leftTurn; _directions[index] != dir; ++index);
    // Then back-off two: while tracing, we can at most turn left 90deg
    index -= leftTurn;
    // std::cout << "OldDir:" << dir.first << "," << dir.second << "  "
    //      << "searchfrom: " << _directions[index].first
    //      << "," << _directions[index].second << std::endl;

    do {
  add(prosp, loc, _directions[index]);
  piter = p.find(prosp);
  ++index;
  assert (index <= 17);
    } while(p.end() == piter);
    // We are guaranteed to find one, unless the the object only has one
    // pixel.
    nextDir = _directions[index-1]; // -1 to reverse the loop increment.

    return piter->first; // return the coord.
}

#endif // POLYGONIZER_H
