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
#include "grouper.h"

#include "mysqlwrapper.h"
#include "cook.h"
#include "cookgroup.h"
#include <string.h>
#include <sstream>
#include <set>
#include <map>
#include <cstdio>
#include <cstdlib>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/make_shared.hpp>
#include <iostream>
/////////////////////////////////////////////////////////////////////
//                For OBSERVATION GROUP COOKING
/////////////////////////////////////////////////////////////////////
using namespace std;

struct ObsPolygonForGroup {
  int obsid;
  int x, y;
};
#define REGION_SIZE 1000
class ScidbCookGroupCallbacks : public CookGroupCallbacks {
public:
  ScidbCookGroupCallbacks (LoggerPtr logger, const std::vector<Image> &images, const std::vector<ObsPos> &allObs)
    : _logger(logger), _images(images), _nextGroupId(0) {
    //cout << "copy of the observations" << allObs.size() << endl;
    _allObsCount = allObs.size();
    _allObsAutoPtr = boost::shared_array<ObsPos> (new ObsPos[allObs.size()]);
    _allObs = _allObsAutoPtr.get();
    memcpy( _allObs, &allObs[0], sizeof(ObsPos) * allObs.size() );
    //cout << "copy success" << endl;
    for (size_t i = 0; i < allObs.size(); ++i) {
      _obsSpatial.push_back (std::map<int, std::vector<int> > ());
    }
    //cout << "retrieving " << _allObsCount << " observations, " << _allObsPolygonsCount << " polygons.." << endl;
    //MYSQL_RES *result = execSelectSQL(_mysql, "SELECT obsid,time,centerx,centery FROM observations ORDER BY obsid", _logger, true /*to save memory*/);
    int count = 0;
    int prevTime = -1;
    std::set <int> finishedTimes;
    for (size_t i=0 ; i < allObs.size(); i++) {
      _allObs [count].obsid = allObs[i].obsid;
      _allObs [count].time = allObs[i].time;
      _allObs [count].centerx = allObs[i].centerx;
      _allObs [count].centery = allObs[i].centery;
      if (prevTime != allObs[i].time) {
        assert (finishedTimes.find (allObs[i].time) == finishedTimes.end()); // should be time clustered
        _timeIndexes [allObs[i].time] = count;
        //cout << "_timeIndexes[" << allObs[i].time << "]=" << count << endl;
        prevTime = allObs[i].time;
        finishedTimes.insert (allObs[i].time);
      }
      int region = toRegion(allObs[i].centerx, allObs[i].centery);
      std::map<int, std::vector<int> > &spatialMap = _obsSpatial[allObs[i].time];
      if (spatialMap.find (region) == spatialMap.end()) {
        spatialMap[region] = std::vector<int> ();
      }
      spatialMap[region].push_back (count);
      ++ count;
    }
    assert (count == _allObsCount);

    /*
    _allObsPolygonsCount = execSelectIntSQL(_mysql, "SELECT COUNT(*) FROM obs_polygons", 0, _logger);
    _allObsPolygonsAutoPtr = boost::shared_array<ObsPolygonForGroup> (new ObsPolygonForGroup[_allObsPolygonsCount]);
    _allObsPolygons = _allObsPolygonsAutoPtr.get();
    ::memset (_allObsPolygons, 0, sizeof (ObsPolygonForGroup) * _allObsPolygonsCount);
    MYSQL_RES *result2 = execSelectSQL(_mysql, "SELECT obsid,x,y FROM obs_polygons ORDER BY obsid,ord", _logger, true );
    
  
    int count2 = 0;
    for (MYSQL_ROW row = ::mysql_fetch_row (result2); row != NULL; row = ::mysql_fetch_row (result2)) {
      _allObsPolygons [count2].obsid = ::atol(row[0]);
      _allObsPolygons [count2].x = ::atol(row[1]);
      _allObsPolygons [count2].y = ::atol(row[2]);
      ++ count2;
    }
    ::mysql_free_result (result2);
    
    assert (count2 == _allObsPolygonsCount);
    */
    //cout << "retrieved " << _allObsCount << " observations." << endl;

    //getAllGroups();
  }

  void getCandidateMatches(std::vector<ObsPos> &observations,
    float D2, int T, int originTime,
    float startx, float starty, float endx, float endy) {
    if (originTime <= 0) return;
    if (T > originTime) T = originTime;
    int totalCnt = 0, positiveCnt = 0;
/*
    uses on memory map as below instead of issuing SQL each time
    sql << "SELECT obsid FROM observations"
      << " WHERE time BETWEEN " << (originTime - T) << " AND " << (originTime - 1)
      << " AND Contains(GeomFromText('Polygon(("
      << (startx - D2 * T) << " " << (starty - D2 * T) << ","
      << (endx + D2 * T) << " " << (starty - D2 * T) << ","
      << (endx + D2 * T) << " " << (endy + D2 * T) << ","
      << (startx - D2 * T) << " " << (endy + D2 * T) << ","
      << (startx - D2 * T) << " " << (starty - D2 * T)
      << "))'), center)";
    }
*/
    for (int backtime = 1; backtime <= T; ++backtime) {
      int time = originTime - backtime;
      for (int yRegion = (starty- D2 * backtime) / REGION_SIZE; yRegion <= (endy + D2 * backtime) / REGION_SIZE; ++yRegion) {
        for (int xRegion = (startx - D2 * backtime) / REGION_SIZE; xRegion <= (endx + D2 * backtime) / REGION_SIZE; ++xRegion) {
          int region = toRegion(xRegion * REGION_SIZE, yRegion * REGION_SIZE);
          const std::map<int, std::vector<int> > &spatialMap = _obsSpatial[time];
          std::map<int, std::vector<int> >::const_iterator it = spatialMap.find (region);
          if (it != spatialMap.end()) {
            for (size_t k = 0; k < it->second.size(); ++k) {
              ++totalCnt;
              int ind = it->second[k];
              const ObsPos &pos = _allObs[ind];
              assert (pos.time == time);
              assert (region == toRegion(pos.centerx, pos.centery));
              float xSqDist = std::min((pos.centerx - startx) * (pos.centerx - startx), (pos.centerx - endx) * (pos.centerx - endx));
              if (startx <= pos.centerx && pos.centerx <= endx) xSqDist = 0;
              float ySqDist = std::min((pos.centery - starty) * (pos.centery - starty), (pos.centery - endy) * (pos.centery - endy));
              if (starty <= pos.centery && pos.centery <= endy) ySqDist = 0;
              const float EPSILON = 0.0001f;
              if (xSqDist + ySqDist <= D2 * (originTime - pos.time) * D2 * (originTime - pos.time) + EPSILON) {
                ++positiveCnt;
                observations.push_back (pos);
              }
            }
          }
        }
      }
    }
    //cout << "totalCnt=" << totalCnt << ", positiveCnt=" << positiveCnt << endl;
  }

  void getObservationsInImage(std::vector<ObsPos> &observations, int imageId) {
    std::map<int, int>::const_iterator it = _timeIndexes.find(imageId);
    assert (it != _timeIndexes.end());
    assert (it->second >= 0);
    assert (it->second < _allObsCount);
    for (int index = it->second; index < _allObsCount && _allObs[index].time == imageId; ++index) {
      observations.push_back (_allObs[index]);
    }
  }
  ImagePos getImagePos (int imageId) {
    assert (imageId >= 0);
    assert (imageId < (int) _images.size());
    const Image &image = _images[imageId];
    ImagePos ret;
    ret.id = image.imageid;
    assert (ret.id == imageId);
    ret.time = image.time;
    ret.startx = image.xstart;
    ret.endx = image.xend;
    ret.starty = image.ystart;
    ret.endy = image.yend;
    return ret;
  }
  void onNewMatches (const std::vector<ObsMatch> &matches) {
    //cout << "newly found " << matches.size() << " pairs" << endl;
    int count = matches.size();
    boost::shared_array<int> newObsids(new int[count]);
    int *newObsidsRaw = newObsids.get();
    ::memset (newObsidsRaw, 0, sizeof (int) * count);
    boost::shared_array<int> existingObsids(new int[count]);
    int *existingObsidsRaw = existingObsids.get();
    ::memset (existingObsidsRaw, 0, sizeof (int) * count);
    _pairsInThisCycleCount.push_back (count);
    _pairsInThisCycleNewObsId.push_back (newObsids);
    _pairsInThisCycleExistingObsId.push_back (existingObsids);
    for (int i = 0; i < count; ++i) {
      const ObsMatch &match = matches[i];
      newObsidsRaw[i] = match.newObsid;
      existingObsidsRaw[i] = match.existingObsid;
    }
  }

  int getNextGroupId () const { return _nextGroupId; }
  void setNextGroupId (int nextGroupId) { _nextGroupId = nextGroupId; }
  struct GroupInfo {
    GroupInfo () {}
    GroupInfo (int groupIdArg, const std::vector <int>& obsIdsArg) : groupId(groupIdArg), obsIds(obsIdsArg) {}
    int groupId;
    std::vector <int> obsIds;
  };
  void processCurrentCycle () {
    // construct what to write back to mysql.
    //cout << "accumulating groups..." << endl;
    for (int timeOffset = 0; timeOffset < (int) _pairsInThisCycleCount.size() ; ++timeOffset) {
      int pairCount = _pairsInThisCycleCount [timeOffset];
      int *pairsNewObsIds = _pairsInThisCycleNewObsId [timeOffset].get();
      int *pairsExistingObsIds = _pairsInThisCycleExistingObsId [timeOffset].get();

      // first, construct pairs for each new observations
      std::map <int, std::vector<int> > newPairs; // map<newobsid, existing>
      for (int i = 0; i < pairCount; ++i) {
        int newObsid = pairsNewObsIds[i];
        int existingObsid = pairsExistingObsIds[i];
        if (newPairs.find (newObsid) == newPairs.end()) {
          newPairs[newObsid] = std::vector<int> ();
        }
        newPairs[newObsid].push_back (existingObsid);
      }

      // process pairs for each *new* obs
      for (std::map <int, std::vector<int> >::const_iterator it = newPairs.begin(); it != newPairs.end(); ++it) {
        int newObsid = it->first;
        const std::vector<int> existingObsids = it->second;
        // take union of existing groups
        std::set<int> groupIds;
        for (size_t i = 0; i < existingObsids.size(); ++i) {
          int existingObsid = existingObsids[i];
          if (groupsTo.find (existingObsid) != groupsTo.end()) {
            // merge into existing groups. existingObsid is not affected
            const std::vector<int> &existingGroupIds = groupsTo [existingObsid];
            groupIds.insert (existingGroupIds.begin(), existingGroupIds.end());
          } else {
            // form a new group. both existingObsid and newObsid are affected
            int groupId = _nextGroupId++;
            groupIds.insert (groupId);
            std::vector<int> thePair;
            thePair.push_back (existingObsid);
            groupsFrom [groupId] = thePair;
            std::vector<int> groupIds;
            groupIds.push_back (groupId);
            groupsTo [existingObsid] = groupIds;
          }
        }
        // and then append to each group (otherwise, we might append the new observation twice to a group)
        for (std::set<int>::const_iterator jt = groupIds.begin(); jt != groupIds.end(); ++jt) {
          int groupId = *jt;
          groupsFrom[groupId].push_back (newObsid);
        }
        groupsTo [newObsid] = std::vector<int> (groupIds.begin(), groupIds.end());
      }
    }
    _pairsInThisCycleNewObsId.clear();
    _pairsInThisCycleExistingObsId.clear();
    _pairsInThisCycleCount.clear();
    //cout << "accumulated groups. " << groupsFrom.size() << " groups." << endl;
  }
 
  std::map<int, std::vector<int> > getGroups()
  {
    return groupsFrom;
  }
  void dumbData(){
    for (std::map<int, std::vector<int> >::const_iterator it = groupsFrom.begin(); it != groupsFrom.end(); ++it) {
      int groupId = it->first;
      groupId = groupId;
      for (size_t j = 0; j < it->second.size(); ++j) {
        int obsid = it->second[j];
        obsid = obsid;
        //cout << "##########" << groupId << " "<<  obsid <<endl;
      }
    }
  } 
  void dumpData() {
    //CsvBuffer csvGr (_logger, "obsgroup", 1 << 22); // custom geomtext csv
    //CsvBuffer csvGrob (_logger, "obsgroup_obs", 1 << 20); // usual csv
    //CsvBuffer csvGrtr (_logger, "polygon_trajectory", 1 << 22); // custom geomtext csv

    for (std::map<int, std::vector<int> >::const_iterator it = groupsFrom.begin(); it != groupsFrom.end(); ++it) {
      int groupId = it->first;
      groupId = groupId;

      // sort by and group by time
      std::map <int, std::vector<ObsPos*> > obsidInTime; // map <time, obs>. could be multiple obsid in a time
      for (size_t j = 0; j < it->second.size(); ++j) {
        int obsid = it->second[j];
        int obsidIndex = binarySearchObsId (obsid);
        ObsPos *obs = _allObs + obsidIndex;
        if (obsidInTime.find (obs->time) == obsidInTime.end()) {
          obsidInTime[obs->time] = std::vector<ObsPos*>();
        }
        obsidInTime[obs->time].push_back (obs);
      }

      //csvGr.written(::sprintf (csvGr.curbuf(), "%d;LINESTRING(", groupId));
      //cout << "Group ID " << groupId << endl;
      int prevTime = -1;
      std::vector<ObsPos*> prevObs;
      bool firstObs = true;
      for (std::map <int, std::vector<ObsPos*> >::const_iterator cur = obsidInTime.begin(); cur != obsidInTime.end(); ++cur) {
        const std::vector<ObsPos*> &curObs = cur->second;
        for (size_t j = 0; j < curObs.size(); ++j) {
          //ObsPos *obs = curObs[j];
          //csvGr.written(::sprintf (csvGr.curbuf(), firstObs ? "%d %d" : ",%d %d", obs->centerx, obs->centery));
          //cout << obs->centerx << " "<< obs->centery << "   --- ";
          if (firstObs) firstObs = false;
          //csvGrob.written(::sprintf (csvGrob.curbuf(), "%d,%d\n", groupId, obs->obsid));
          //cout << obs-> obsid << " Time: "<< cur->first << endl;
        }
        /* 
        if (prevTime >= 0) { // write polygon trajectory
          csvGrtr.written(::sprintf (csvGrtr.curbuf(), "%d;%d;%d;POLYGON((", groupId, prevTime, cur->first));
          int firstX = -1, firstY = -1;
          bool first = true;
          for (size_t j = 0; j < prevObs.size(); ++j) {
            int obsid = prevObs[j]->obsid;
            int polygonIndex = binarySearchObsIdPolygons (obsid);
            for (int k = polygonIndex; k < _allObsPolygonsCount && _allObsPolygons[k].obsid == obsid; ++k) {
              csvGrtr.written(::sprintf (csvGrtr.curbuf(), "%d %d,", _allObsPolygons[k].x, _allObsPolygons[k].y));
              if (first) {
                firstX = _allObsPolygons[k].x;
                firstY = _allObsPolygons[k].y;
                first = false;
              }
            }
          }
          for (size_t j = 0; j < curObs.size(); ++j) {
            int obsid = curObs[j]->obsid;
            int polygonIndex = binarySearchObsIdPolygons (obsid);
            for (int k = polygonIndex; k < _allObsPolygonsCount && _allObsPolygons[k].obsid == obsid; ++k) {
              csvGrtr.written(::sprintf (csvGrtr.curbuf(), "%d %d,", _allObsPolygons[k].x, _allObsPolygons[k].y));
            }
          }
          csvGrtr.written(::sprintf (csvGrtr.curbuf(), "%d %d))\n", firstX, firstY));
        }
        */
        prevTime = cur->first;
        prevObs = cur->second;
      }
      prevTime = prevTime;
      //csvGr.written(::sprintf (csvGr.curbuf(), ")\n"));
    }
    //csvGr.close ();
    //csvGrob.close ();
    //csvGrtr.close ();
    //cout << "wrote to CSV." << endl;
    // csvGr/csvGrtr uses custom csv loading
    // LOAD DATA REPLACE for in-place-update is slow. Instead of that,
    // let's just re-make the table. We anyway have all data here.
    /*
    ObservationGroupTable groupTable(_mysql);
    groupTable.dropIfExists();
    groupTable.createTable();
    csvGrob.loadToMysql(_mysql); // csvGrob uses usual loading
    execUpdateSQL (_mysql, "LOAD DATA LOCAL INFILE '" + csvGr._csvname + "' INTO TABLE "
      + csvGr._tablename + " FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'"
      + "(obsgroupid,@var1) SET center_traj=GeomFromText(@var1)", _logger);
    execUpdateSQL (_mysql, "LOAD DATA LOCAL INFILE '" + csvGrtr._csvname + "' INTO TABLE "
      + csvGrtr._tablename + " FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'"
      + "(obsgroupid,fromtime,totime,@var1) SET traj=GeomFromText(@var1)", _logger);
    */
  }

private:
  LoggerPtr _logger;
  std::map <int, int> _timeIndexes;
  const std::vector<Image> &_images;
  int _nextGroupId;

  // shared_array + raw_ptr idiom
  boost::shared_array<ObsPos> _allObsAutoPtr;
  ObsPos *_allObs;
  int _allObsCount;
  // use on memory indexing rather querying mysql each time
  std::vector<std::map<int, std::vector<int> > > _obsSpatial; // vector<time> : map <region, obsPositions(not obsid!)>

  boost::shared_array<ObsPolygonForGroup> _allObsPolygonsAutoPtr;
  ObsPolygonForGroup *_allObsPolygons;
  int _allObsPolygonsCount;

  std::vector<boost::shared_array<int> > _pairsInThisCycleNewObsId;
  std::vector<boost::shared_array<int> > _pairsInThisCycleExistingObsId;
  std::vector<int> _pairsInThisCycleCount;

  std::map<int, std::vector<int> > groupsFrom; //map <groupid, vector<obsid> >
  std::map<int, std::vector<int> > groupsTo; //map <obsid, vector<groupid> >

  // returns the index in _allObs for given obsid
  int binarySearchObsId (int obsid) const {
    int low = 0;
    int high = _allObsCount - 1;
    while (low <= high) {
      int mid = (low + high) / 2;
      int midVal = _allObs[mid].obsid;
      if (midVal < obsid) low = mid + 1;
      else if (midVal > obsid) high = mid - 1;
      else return mid; // key found
    }
    assert (false);
    for(int i=0;i<_allObsCount;i++)
    if(_allObs[i].obsid == obsid)
	return i;
    return -1;  // key not found.
  }

  // returns the *first* index in _allObsPolygons for given obsid
  int binarySearchObsIdPolygons (int obsid) const {
    int low = 0;
    int high = _allObsPolygonsCount - 1;
    int index = -1;
    while (low <= high) {
      int mid = (low + high) / 2;
      int midVal = _allObsPolygons[mid].obsid;
      if (midVal < obsid) low = mid + 1;
      else if (midVal > obsid) high = mid - 1;
      else {
        index = mid;
        break; // key found
      }
    }
    assert (index >= 0);
    // this might not be the first one, so backtrack
    int bindex;
    for (bindex = index; bindex >= 0 && _allObsPolygons[bindex].obsid == obsid; --bindex);
    assert (_allObsPolygons[bindex + 1].obsid == obsid);
    assert (bindex == -1 || _allObsPolygons[bindex].obsid != obsid);
    return bindex + 1;
  }
  /*
  void getAllGroups () {
    MYSQL_RES *result = execSelectSQL(_mysql, "SELECT obsgroupid,obsid FROM obsgroup_obs ORDER BY obsgroupid,obsid", _logger);
    int currentGroupId = -1;
    int cnt = 0;
    std::vector<int> currentObsids;
    for (MYSQL_ROW row = ::mysql_fetch_row (result); row != NULL; row = ::mysql_fetch_row (result)) {
      int groupid = ::atol(row[0]);
      int obsid = ::atol(row[1]);
      if (currentGroupId != groupid) {
        if (currentGroupId >= 0) {
          groupsFrom [currentGroupId] = currentObsids;
          ++cnt;
        }
        currentGroupId = groupid;
        currentObsids.clear();
      }
      currentObsids.push_back (obsid);
      if (groupsTo.find (obsid) == groupsTo.end()) {
        groupsTo[obsid] = std::vector<int>();
      }
      groupsTo[obsid].push_back (groupid);
    }
    if (currentGroupId >= 0) {
      groupsFrom [currentGroupId] = currentObsids;
      ++cnt;
    }
    ::mysql_free_result (result);
    cout << "retrieved " << cnt << " groups...");
  }
  */
  inline int toRegion (int x, int y) {
    assert (x >= 0);
    assert (x < (REGION_SIZE << 16));
    return ((y / REGION_SIZE) << 16) + (x / REGION_SIZE);
  }
};

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
void Grouper::loadGroup(const std::vector<ObsPos> &_allObs, const std::vector<Image> &allImages, float D2, int T){ 
  static log4cxx::LoggerPtr _logger(log4cxx::Logger::getLogger("Groupstars"));
  allObs= _allObs;
  allObsCount= allObs.size();
  //cout << "grouping observations... D2(max velocity [cells/time])=" << D2 << ", T(max backtracking [time])=" << T << endl;
  {
    //cout << " create the callbacks" << endl;
    ScidbCookGroupCallbacks callbacks (_logger, allImages, allObs);
    //cout << " create the cookGroup function" << endl;
    CookGroup cookGroup (callbacks, D2, T);
    // we cook and insert to mysql per cycle as specified in the benchmark spec.
    // the most efficient way is to cook all cycles and insert to mysql at once,
    // but it doesn't reflect what the astronomy project actually does.
    for (int cycle = 0; (int) allImages.size() - cycle * 20 > 0; ++cycle) {
      //cout << "Cooking groups in " << cycle << "th cycle..." << endl;
      // all groups found in this cycle are inserted to mysql together for efficiency
      cookGroup.cook(cycle * 20, std::min((cycle + 1) * 20, (int) allImages.size()));
      callbacks.processCurrentCycle();
      //cout << "Done cycle=" << cycle << endl;;
    }
    //callbacks.dumbData();
    //callbacks.dumpData();	
    groups= callbacks.getGroups();
    //cout << "loaded groups. max groupid=" << callbacks.getNextGroupId() << endl;
  }
  //cout << "deleted RdbmsCookGroupCallbacks" << endl;
  //cout << "finished grouping observations." << endl;
}
  // returns the index in _allObs for given obsid
  int Grouper::binarySearchObsId (int obsid) const {
    int low = 0;
    int high = allObsCount - 1;
    while (low <= high) {
      int mid = (low + high) / 2;
      int midVal = allObs[mid].obsid;
      if (midVal < obsid) low = mid + 1;
      else if (midVal > obsid) high = mid - 1;
      else return mid; // key found
    }
    assert (false);
    for(int i=0;i<allObsCount;i++)
    if(allObs[i].obsid == obsid)
        return i;
    return -1;  // key not found.
  }
void Grouper::storeGroup(boost::shared_ptr<MemArray> output)
{
  static log4cxx::LoggerPtr _logger(log4cxx::Logger::getLogger("Groupstars"));
  _oidIterator= output->getIterator(0);
  _xIterator= output->getIterator(1);
  _yIterator= output->getIterator(2);
  // Go over each group ID
  for (std::map<int, std::vector<int> >::const_iterator it = groups.begin(); it != groups.end(); ++it) {
    int groupId = it->first;
    int currentObs=0;
    //Sort the observation by Time
    std::map <int, std::vector<ObsPos*> > obsidInTime; // map <time, obs>. could be multiple obsid in a time
    for (size_t j = 0; j < it->second.size(); ++j) {
        int obsid = it->second[j];
        int obsidIndex = binarySearchObsId (obsid);
        ObsPos *obs =&allObs[obsidIndex];
        if (obsidInTime.find (obs->time) == obsidInTime.end()) {
          obsidInTime[obs->time] = std::vector<ObsPos*>();
        }
        obsidInTime[obs->time].push_back (obs);
    }
    //The Real work
    for (std::map <int, std::vector<ObsPos*> >::const_iterator cur = obsidInTime.begin(); cur != obsidInTime.end(); ++cur) {
      const std::vector<ObsPos*> &curObs = cur->second;
      for (size_t j = 0; j < curObs.size(); ++j) {
        Coordinates pos(2);
        ObsPos *obs = curObs[j];
        //if (firstObs) firstObs = false;
        pos[0]= groupId;
        pos[1]= currentObs;
        //LOG4CXX_DEBUG(_logger, "Setting OID:" << obs->obsid << " @" << pos[0] << ":" << pos[1]);
        //Store in the respective chunk
        Value item(TypeLibrary::getType(TID_INT64));
        setOutputPosition(_oidIterator,_oidChunkit, pos);
        setOutputPosition(_xIterator,_xChunkit, pos);
        setOutputPosition(_yIterator,_yChunkit, pos);
        item.setInt64(obs->obsid);
        _oidChunkit->writeItem(item); 
        item.setInt64(obs->centerx);
        _xChunkit->writeItem(item); 
        item.setInt64(obs->centery);
        _yChunkit->writeItem(item); 
        currentObs++;
      }
    }
  }
  // Finalize
  if (_oidChunkit) _oidChunkit->flush();
  if (_xChunkit) _xChunkit->flush();
  if (_yChunkit) _yChunkit->flush();
}
