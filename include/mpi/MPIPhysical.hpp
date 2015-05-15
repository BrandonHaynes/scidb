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

#ifndef MPIPHYSICAL_HPP_
#define MPIPHYSICAL_HPP_
///
/// @file MPIPhysical.hpp
///
///

// scidb
#include <array/Array.h>
#include <array/Metadata.h>
#include <query/Query.h>
#include <query/Operator.h>
#include <system/Cluster.h>

// scidb
#include <mpi/MPIUtils.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>

namespace scidb {
using namespace boost;

inline Coordinates getStartMin(Array* a) {
    Coordinates result(2);
    result[0] = a->getArrayDesc().getDimensions()[0].getStartMin();
    result[1] = a->getArrayDesc().getDimensions()[1].getStartMin();
    return result;
}
inline Coordinates getEndMax(Array* a) {
    Coordinates result(2);
    result[0] = a->getArrayDesc().getDimensions()[0].getEndMax();
    result[1] = a->getArrayDesc().getDimensions()[1].getEndMax();
    return result;
}

static double getNan()
{
    static double S_nanVal=0.0;
    if(S_nanVal == 0.0) {
        S_nanVal = ::nan("");
    }
    return S_nanVal;
}

static double getTimingSec()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
    }
    return (ts.tv_sec + ts.tv_nsec*1e-9);
}

class Timing {
public:
                Timing()
                :
                    _startSec(getTimingSec()),
                    _stopSec(getNan())
                {}
    void        start() { _startSec = getTimingSec(); }
    double      stop() { _stopSec = getTimingSec(); return operator double(); }
                operator double() { return _stopSec - _startSec; }
private:
    double      _startSec;
    double      _stopSec;
};

// handy inline
inline bool doCerrTiming()
{
#if !defined(NDEBUG) && defined(SCALAPACK_TIMING)
    return true;
#else
    return false;
#endif
}

void throwIfDegradedMode(shared_ptr<Query>& query);



class MPIPhysical : public PhysicalOperator
{
public:
    MPIPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }
    virtual void setQuery(const boost::shared_ptr<Query>& query);

    virtual void postSingleExecute(shared_ptr<Query> query);

protected:
    // TODO James : speak to Tigor about whether these methods might be implemented on the _ctx instead of around it.

    /**
     *  Launches a new set of MPI processes
     *  May be orchestrated from only 1 of the N processes, such as from the coordinator
     *  Caller should not depend on that.
     *  @param query
     *  @param maxSlaves
     *  @xxxxx (sets state): _launchId, _mustLaunch, _ctx, _launcher, _ipcName
     *  @return true if this instance participates in the computation and should handshake with a slave; false otherwise
     */
    bool launchMPISlaves(shared_ptr<Query>& query, const size_t maxSlaves);

    /** @param numBufs : how many to allocate
     *  @param elemSize : sizeof(float) or sizeof(double) or sizeof(complex)
     *  @param sizes : how large to make each buffer
     *  @param fill value (could make this a template function)
     *  @returns std::vector<SMIptr_t> shmIpc(NUM_BUFS)
     */
    typedef boost::shared_ptr<SharedMemoryIpc> SMIptr_t ;
    std::vector<SMIptr_t> allocateMPISharedMemory(size_t numBufs, size_t  elemSize[], size_t sizes[],
                                                  string dbgNames[]);
    /**
     * Release shared memory regions and associated resources which are not used in constructing the result array
     * @param shmIpc list of shm regions
     * @param resultIpcIndx index into shmIpc of the region which must stay mapped (because it is used in the array/query pipeline)
     */
    void releaseMPISharedMemoryInputs(std::vector<MPIPhysical::SMIptr_t>& shmIpc, size_t resultIpcIndx);

    /// Cleanup the context created by launchMPISlaves()
    void unlaunchMPISlaves() {
        if (!_mustLaunch) {
            _ctx.reset();
            assert(!_launcher);
        }
    }

    /// Cleanup the context created by launchMPISlaves() on the instance not participating in the launch
    void unlaunchMPISlavesNonParticipating() {
        unlaunchMPISlaves();
    }

    // TODO JHM  to discuss with Tigor
    // it takes these 5 variables for an operator to launch an manage a slave.
    // it would be better if launching a slave returned a reference to a single slave object
    // (whose ownership may still remain with the _ctx)
    // to which one can attach shared memory, do handshakes, get results, etc without
    // having to track these 5 variables.  The client would like a single handle from
    // which to manage child operations.
    // This would be provided as the first argument to xxxxMaster() calls to simplify them
    // as well.
protected:
    uint64_t                              _launchId;	// would like the MpiOperatorContext to track this
    std::string				  _ipcName;
    boost::shared_ptr<MpiOperatorContext> _ctx;
    private:
    bool				  _mustLaunch;  // would like the MpiOperatorContext to track this
    boost::shared_ptr<MpiLauncher>        _launcher;    // move to MpiOperatorContext
};




} // namespace


#endif /* MPIPHYSICAL_HPP_ */
