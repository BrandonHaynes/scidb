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
#ifndef COOK_GROUP_H
#define COOK_GROUP_H

#include <vector>
#include <sys/types.h>

// Group Cooking functions to find pairs of observations and group them.
// These functions are in more "batch"ish style than cook.h because
// group cooking far more depends on existing data (e.g., existing group).
// Everything is provided as a huge batch, and users must provide 
// data in a batch to efficiently process cooking and storing the results.

// compact structs that represent an observation, image and group
struct ObsPos {
  ObsPos(){}
  ObsPos(int _obsid,int _time, int _centerx, int _centery) : obsid(_obsid), time(_time), centerx(_centerx), centery(_centery) {}
  int obsid;
  int time;
  int centerx;
  int centery;
};

struct ImagePos {
  int id;
  int time;
  int startx, starty, endx, endy;
};

struct ObsMatch {
  ObsMatch (int newObsid_, int existingObsid_) : newObsid(newObsid_), existingObsid(existingObsid_) {}
  int newObsid;
  int existingObsid;
};

// callback interface for group cooking
class CookGroupCallbacks {
public:
  // *this will be called once for each image to be cooked*
  // searches a set of observations that could potentially match with
  // some observation in the given ranges.
  // such candidates could exist in 3-D (x/y/time) trapezoidal areas
  // starting from (x=startx/endx +- D2, y=starty/endy +- D2, time=originTime-1)
  // to (x=startx/endx +- D2*T, y=starty/endy +- D2*T, time=originTime-T)
  // note: okay to have false positives, but don't have false negatives. (e.g., okay to return ALL existing)
  virtual void getCandidateMatches(
    std::vector<ObsPos> &observations,
    float D2, int T, int originTime,
    float startx, float starty, float endx, float endy) = 0;

  // *this will be called once for each image to be cooked*
  // returns all observations in given image (specified by imageid).
  // this function will be called only for "new" images that are currently
  // processed in the cycle.
  virtual void getObservationsInImage(std::vector<ObsPos> &observations, int imageId) = 0;

  // returns the dimensions of given image
  // this doesn't matter in terms of performance
  virtual ImagePos getImagePos (int imageId) = 0;

  // *this will be called once for each image to be cooked*
  // reports pairs of new and existing observations.
  // it's *your* responsibility to merge this information to group,
  // issue new group-id if needed, etc. This class just reports pairs.
  virtual void onNewMatches (const std::vector<ObsMatch> &matches) = 0;
};

class CookGroup {
public:
  // D2(max velocity [cells/time]), T(max backtracking [time]
  CookGroup (CookGroupCallbacks &callbacks, float D2, int T) : _callbacks(callbacks), _D2(D2), _T (T), _totalPairCnt(0) {}

  // find groups from specified images
  // you could call this function with from=0, to=last for just once,
  // or with more small "cycles" as specified in the document, depending on the data size.
  void cook (int fromImageId, int toImageId);

  int64_t getTotalPairCnt () const { return _totalPairCnt; }
private:
  void matchObservations(const std::vector<ObsPos> &candidates, const std::vector<ObsPos> &newObservations,
    int originTime);

  CookGroupCallbacks &_callbacks;
  float _D2;
  int _T;
  int64_t _totalPairCnt;
};

#endif //COOK_GROUP_H
