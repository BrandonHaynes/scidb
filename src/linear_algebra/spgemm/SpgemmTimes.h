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
#ifndef SPGEMM_TIMES_H
#define SPGEMM_TIMES_H

/*
 * PhysicalSpgemm.h
 *
 *  Created on: June 27, 2014
 */

// C++
#include <limits>
#include <limits>
#include <sstream>

// boost

// scidb

// local

namespace scidb
{
using namespace boost;
using namespace scidb;

enum dummy {DBG_TIMING=1}; // 0 to disable

double getDbgMonotonicrawSecs()
{
    if(DBG_TIMING) {  // only during special debugging
        // TODO: refactor as getSecs(MONOTONIC_RAW) etc
        struct timespec ts;
        if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
        }
        return double(ts.tv_sec + 1e-9 * ts.tv_nsec);
    } else {
        return 0.0;
    }
}

double getDbgThreadSecs()
{
    if(DBG_TIMING) {  // only during special debugging
        struct timespec ts;
        if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
        }
        return double(ts.tv_sec + 1e-9 * ts.tv_nsec);
    } else {
        return 0.0;
    }
}


// for performance analysis
struct SpgemmTimes {
    SpgemmTimes() : redistributeLeftSecs(0.0), totalSecs(0.0) {;}

    void nextRound() {
        redistributeRightSecs.push_back(0) ;
        loadRightSecs.push_back(0) ;
        loadLeftSecs.push_back(0) ;
        loadLeftCopySecs.push_back(0) ;
        blockMultFindRightSecs.push_back(0);
        blockMultSecs.push_back(0) ;
        blockMultSPAFlushSecs.push_back(0);
            blockMultSPAFlushSortSecs.push_back(0);
            //blockMultSPAFlushTopHalfSecs.push_back(0);
            //blockMultSPAFlushSetValWriteItemSecs.push_back(0);
            blockMultSPAFlushClearSecs.push_back(0);
        //blockMultLoopOverheadSecs.push_back(0) ;
        blockMultSubtotalSecs.push_back(0);
        flushSecs.push_back(0) ;
        roundSubtotalSecs.push_back(0) ;
    }

    // not per round
    double _totalSecsStart;
    void totalSecsStart() { _totalSecsStart = getDbgMonotonicrawSecs(); }
    void totalSecsStop()  { totalSecs       = getDbgMonotonicrawSecs() - _totalSecsStart ; }

    // not per round?
    double _redistLeftStart;
    void redistLeftStart() { _redistLeftStart = getDbgMonotonicrawSecs(); }
    void redistLeftStop() { redistributeLeftSecs = (getDbgMonotonicrawSecs()-_redistLeftStart) ; }

    double _redistRightStart;
    void redistRightStart() { _redistRightStart = getDbgMonotonicrawSecs(); }
    void redistRightStop()  { redistributeRightSecs.back() += getDbgMonotonicrawSecs() - _redistRightStart ; }

    double _loadRightStart;
    void loadRightStart() { _loadRightStart = getDbgMonotonicrawSecs(); }
    void loadRightStop()  { loadRightSecs.back() += getDbgMonotonicrawSecs() - _loadRightStart; }

    double _loadLeftCopyStart;
    void loadLeftCopyStart() { _loadLeftCopyStart = getDbgMonotonicrawSecs(); }
    void loadLeftCopyStop() { loadLeftCopySecs.back() += getDbgMonotonicrawSecs() - _loadLeftCopyStart; }

    double _loadLeftStart;
    void loadLeftStart() { _loadLeftStart = getDbgMonotonicrawSecs(); }
    void loadLeftStop() { loadLeftSecs.back() += getDbgMonotonicrawSecs() - _loadLeftStart; }

    double _blockMultFindRightStart;
    void blockMultFindRightStart() { _blockMultFindRightStart = getDbgMonotonicrawSecs(); }
    void blockMultFindRightStop()  { blockMultFindRightSecs.back() += getDbgMonotonicrawSecs() - _blockMultFindRightStart; }

    double _blockMultStart;
    void blockMultStart() { _blockMultStart = getDbgMonotonicrawSecs(); }
    void blockMultStop()  { blockMultSecs.back() += getDbgMonotonicrawSecs() - _blockMultStart; }

    double _blockMultSPAFlushStart;
    void blockMultSPAFlushStart() { _blockMultSPAFlushStart = getDbgMonotonicrawSecs(); }
    void blockMultSPAFlushStop()  { blockMultSPAFlushSecs.back() += getDbgMonotonicrawSecs() - _blockMultSPAFlushStart; }

    double _blockMultSPAFlushSortStart;
    void blockMultSPAFlushSortStart() { _blockMultSPAFlushSortStart = getDbgMonotonicrawSecs(); }
    void blockMultSPAFlushSortStop()  { blockMultSPAFlushSortSecs.back() += getDbgMonotonicrawSecs() - _blockMultSPAFlushSortStart; }

    double _blockMultSPAFlushClearStart;
    void blockMultSPAFlushClearStart() { _blockMultSPAFlushClearStart = getDbgMonotonicrawSecs(); }
    void blockMultSPAFlushClearStop()  { blockMultSPAFlushClearSecs.back() += getDbgMonotonicrawSecs() - _blockMultSPAFlushClearStart; }

    double _blockMultSubtotalStart;
    void blockMultSubtotalStart() { _blockMultSubtotalStart = getDbgMonotonicrawSecs(); }
    void blockMultSubtotalStop()  { blockMultSubtotalSecs.back() += getDbgMonotonicrawSecs() - _blockMultSubtotalStart; }

    double _flushStart;
    void flushStart() { _flushStart = getDbgMonotonicrawSecs(); }
    void flushStop()  { flushSecs.back() += getDbgMonotonicrawSecs() - _flushStart; }

    double _roundSubtotalStart;
    void roundSubtotalStart() { _roundSubtotalStart = getDbgMonotonicrawSecs(); }
    void roundSubtotalStop()  { roundSubtotalSecs.back() += getDbgMonotonicrawSecs() - _roundSubtotalStart; }



    double                  redistributeLeftSecs;

                            // multiple per-psByCol rotation values
    std::vector<double>     redistributeRightSecs;

    std::vector<double>     loadRightSecs;
    std::vector<double>     loadLeftCopySecs;
    std::vector<double>     loadLeftSecs;
    std::vector<double>     blockMultFindRightSecs;
    std::vector<double>     blockMultSecs;
    std::vector<double>     blockMultSPAFlushSecs;
        std::vector<double>     blockMultSPAFlushSortSecs;
        //std::vector<double>     blockMultSPAFlushTopHalfSecs;
        //std::vector<double>     blockMultSPAFlushSetValWriteItemSecs;
        std::vector<double>     blockMultSPAFlushClearSecs;
    //std::vector<double>     blockMultLoopOverheadSecs;
    std::vector<double>     blockMultSubtotalSecs;
    std::vector<double>     flushSecs;
    std::vector<double>     roundSubtotalSecs;

    double                  totalSecs;

};

/**
* print a SpgemmTimes on an ostream
* @param os    -- the ostream to print on
* @param times -- the SpgemmTimes structure
* @return      -- the ostream, os, that was passed in
*/
std::ostream& operator<<(ostream& os, const SpgemmTimes& times)
{
    if(DBG_TIMING) {  // only during special debugging
        os << "spgemm(): " << std::endl;
        os << "redistributeLeftSecs:     " << times.redistributeLeftSecs << std::endl ;

        for(size_t ii=0; ii<times.redistributeRightSecs.size(); ++ii) {
            os << "round: " << ii << " --------------" << std::endl ;

            os << "  redistributeRightSecs:    " << times.redistributeRightSecs[ii] << std::endl ;
            os << "  loadRightSecs:            " << times.loadRightSecs[ii] << std::endl ;
            os << "  loadLeftSecs:             " << times.loadLeftSecs[ii] << std::endl ;
            os << "  loadLeftCopySecs:       " << times.loadLeftCopySecs[ii] << std::endl ;
            os << std::endl;
            os << "  blockMultFindRightSecs:   " << times.blockMultFindRightSecs[ii] << std::endl ;
            os << "  blockMultSecs:            " << times.blockMultSecs[ii] << std::endl ;
            os << "  blockMultSPAFlushSecs:    " << times.blockMultSPAFlushSecs[ii] << std::endl ;
            //os << "    blockMultSPAFlushSortSecs:            " << times.blockMultSPAFlushSortSecs[ii] << std::endl ;
            //os << "    blockMultSPAFlushTopHalfSecs:         " << times.blockMultSPAFlushTopHalfSecs[ii] << std::endl ;
            //os << "    blockMultSPAFlushSetValWriteItemSecs: " << times.blockMultSPAFlushSetValWriteItemSecs[ii] << std::endl ;
            //os << "    blockMultSPAFlushClearSecs:           " << times.blockMultSPAFlushClearSecs[ii] << std::endl ;
            // subtract totaled items from inside the loop to show loop overhead
            double loopOverhead = times.blockMultSubtotalSecs[ii]
                                  - times.blockMultSecs[ii]
                                  - times.blockMultSPAFlushSecs[ii] ;
            os << "    blockMultLoopOverheadSecs:            " << loopOverhead << std::endl ;
            os << "  ------------------------" << std::endl;
            os << "  [blockMultSubtotalSecs]:  " << times.blockMultSubtotalSecs[ii] << "]" << std::endl ;
            os << "  flushSecs:                "    << times.flushSecs[ii] << std::endl ;
            os << "  ------------------------" << std::endl;
            os << "  [roundSubtotalSecs]:    " << times.roundSubtotalSecs[ii] << std::endl ;
        }

        os << "--------------------------------" << std::endl ;
        os << " totalSecs: " << times.totalSecs << std::endl ;
    } else {
        os << "timing disabled" << std::endl;
    }

    return os;
}

} // end namespace scidb

#endif // SPGEMM_TIMES_H
