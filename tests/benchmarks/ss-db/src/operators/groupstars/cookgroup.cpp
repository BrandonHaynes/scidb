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
#include "cookgroup.h"
#include <map>
#include <utility>

void CookGroup::cook (int fromImageId, int toImageId) {
  for (int imageId = fromImageId; imageId < toImageId; ++imageId) {
    ImagePos image = _callbacks.getImagePos(imageId);
    std::vector<ObsPos> existingObservations;
    existingObservations.reserve (1 << 14);
    _callbacks.getCandidateMatches(existingObservations,
                        _D2, _T, image.time, image.startx, image.starty, image.endx, image.endy);
    std::vector<ObsPos> newObservations;
    newObservations.reserve (1 << 15);
    _callbacks.getObservationsInImage(newObservations, image.id);
    matchObservations(existingObservations, newObservations, image.time);
  }
}

void CookGroup::matchObservations(const std::vector<ObsPos> &candidates, const std::vector<ObsPos> &newObservations,
  int originTime) {
  // simple spatial clustering (to make it n^2 to nlogn)
  int regionSize = (_D2 * _T) + 1; // make sure matches could be found in +-1 region
  std::map <std::pair<int, int>, std::vector<int> > regions; // map< pair<regionx, regiony>, entries>
  for (size_t i = 0; i < candidates.size(); ++i) {
    const ObsPos &candidate = candidates[i];
    int regionX = candidate.centerx / regionSize;
    int regionY = candidate.centery / regionSize;
    std::pair<int, int> key (regionX, regionY);
    std::map <std::pair<int, int>, std::vector<int> >::iterator it = regions.find (key);
    if (it == regions.end ()) {
      std::vector<int> entries;
      entries.push_back (i);
      regions[key] = entries;
    } else {
      it->second.push_back (i);
    }
  }

  std::vector<ObsMatch> matches;
  matches.reserve (1 << 15);
  for (size_t i = 0; i < newObservations.size(); ++i) {
    const ObsPos &newObs = newObservations[i];
    int regionX = newObs.centerx / regionSize;
    int regionY = newObs.centery / regionSize;
    for (int x = regionX -1; x <= regionX + 1; ++x) {
      for (int y = regionY -1; y <= regionY + 1; ++y) {
        std::pair<int, int> key (x, y);
        std::map <std::pair<int, int>, std::vector<int> >::iterator it = regions.find (key);
        if (it == regions.end ()) continue;
        const std::vector<int> &entries = it->second;
        for (size_t j = 0; j < entries.size(); ++j) {
          const ObsPos &candidate = candidates[entries[j]];
          float xSqDist = (candidate.centerx - newObs.centerx) * (candidate.centerx - newObs.centerx);
          float ySqDist = (candidate.centery - newObs.centery) * (candidate.centery - newObs.centery);
          if (xSqDist + ySqDist <= _D2 * (originTime - candidate.time) * _D2 * (originTime - candidate.time)) {
            matches.push_back (ObsMatch (newObs.obsid, candidate.obsid));
            ++_totalPairCnt;
          }
        }
      }
    }
  }
  _callbacks.onNewMatches(matches);
}
