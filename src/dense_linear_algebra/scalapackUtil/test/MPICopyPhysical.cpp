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

// TODO JHM : It looks like this is actually a ScaLAPACKCopy operator.  It needs to be renamed, and derived off of ScaLAPACKPhysical

// std C++
#include <cmath>
#include <sstream>
#include <string>

// std C

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>

// SciDB
#include <array/MemArray.h>
#include <array/StreamArray.h>
#include <log4cxx/logger.h>
#include <util/Network.h>
#include <query/Operator.h>
#include <query/Query.h>
#include <system/BlockCyclic.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/Utility.h>

// more SciDB
#include "../../array/OpArray.h"
#include "../../dlaScaLA/scalapackEmulation/scalapackEmulation.hpp"
#include <mpi/MPIUtils.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include <mpi/MPIPhysical.hpp>
#include "../../DLAErrors.h"
#include "../../scalapackUtil/reformat.hpp"
#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include "../../scalapackUtil/test/slaving/mpiCopyMaster.hpp"
#include "../../scalapackUtil/test/slaving/mpiCopySlave.hpp"

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.mpicopy"));
static const bool DBG = false;

class MPICopyPhysical : public ScaLAPACKPhysical
{
public:
        MPICopyPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
        :
            ScaLAPACKPhysical(logicalName, physicalName, parameters, schema)
        {
        }

    virtual bool changesDistribution(const std::vector<ArrayDesc> & inputSchemas) const
    {
        return true;
    }


    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psScaLAPACK);
    }
    // if outputting partial blocks that need to be merged, one needs to override
    // another operator called ???

    virtual shared_ptr<Array> execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query);
    virtual shared_ptr<Array>  invokeMPI(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query>& query,
                                         ArrayDesc& outSchema);
};

shared_ptr<Array>  MPICopyPhysical::invokeMPI(std::vector< shared_ptr<Array> >& inputArrays,
                                              shared_ptr<Query>& query,
                                              ArrayDesc& outSchema)
{
    // + intersects the array chunkGrids with the maximum process grid
    // + sets up the ScaLAPACK grid accordingly and if not participating, return early
    // + start and connect to an MPI slave process
    // + create ScaLAPACK descriptors for the input arrays
    // + convert the redistributedInputs into in-memory ScaLAPACK layout in shared memory
    // + call a "master" routine that passes the ScaLAPACK operator name, parameters,
    //   and shared memory descriptors to the ScaLAPACK MPI process that will do the
    //   actual computation.
    // + wait for successful completion
    // + construct an "OpArray" that make and Array API view of the output memory.
    // + return that output array.

    if(DBG) std::cerr << "MPICopyPhysical::invokeMPI(): reached" << std::endl ;

    // MPI_Init(); -- now done in slave processes
    // in SciDB we use query->getInstancesCount() & getInstanceID()
    // and we use a fake scalapack gridinit to set up the grid and where
    // we are in it.  so the blacs_getinfo calls below are fakes to help
    // keep the code more similar

    //
    // Initialize the (emulated) BLACS and get the proces grid info
    //
    bool isParticipatingInScaLAPACK = doBlacsInit(inputArrays, query, "MPICopyPhysical");
    slpp::int_t ICTXT=-1, NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    if (isParticipatingInScaLAPACK) {
        scidb_blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
        checkBlacsInfo(query, ICTXT, NPROW, NPCOL, MYPROW, MYPCOL, "MPICopyPhysical");
    }

    //
    // launch MPISlave if we participate
    // TODO: move this down into the ScaLAPACK code ... something that does
    //       the doBlacsInit, launchMPISlaves, and the check that they agree
    //
    bool isParticipatingInMPI = launchMPISlaves(query, NPROW*NPCOL);
    if (isParticipatingInScaLAPACK != isParticipatingInMPI) {
        LOG4CXX_DEBUG(logger, "MPICopyPhysical::invokeMPI():"
                              << " isParticipatingInScaLAPACK " << isParticipatingInScaLAPACK
                              << " isParticipatingInMPI " << isParticipatingInMPI);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << "MPICopyPhysical::invokeMPI(): internal inconsistency in MPI slave launch.");
    }

    if (isParticipatingInMPI) {
        LOG4CXX_DEBUG(logger, "MPICopyPhysical::invokeMPI(): participating in MPI");
    } else {
        LOG4CXX_DEBUG(logger, "MPICopyPhysical::invokeMPI(): not participating in MPI");

        procRowCol_t firstChunkSize = { chunkRow(inputArrays[0]), chunkCol(inputArrays[0]) };
        shared_ptr<PartitioningSchemaDataForScaLAPACK> schemeData =
        make_shared<PartitioningSchemaDataForScaLAPACK>(getBlacsGridSize(inputArrays, query, "MPICopyPhysical"), firstChunkSize);

        shared_ptr<Array> tmpRedistedInput = redistributeInputArray(inputArrays[0], schemeData, query, "MPICopyPhysical");

        bool wasConverted = (tmpRedistedInput != inputArrays[0]) ;  // only when redistribute was actually done (sometimes optimized away)
        if (wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
        inputArrays[0].reset();
        unlaunchMPISlavesNonParticipating();
        return shared_ptr<Array>(new MemArray(_schema,query));
    }

    // REFACTOR: this is a pattern in DLAs
    //
    // get dimension information about the input arrays
    //
    if(DBG) std::cerr << "invokeMPI get dim info" << std::endl ;
    boost::shared_ptr<Array> Ain = inputArrays[0];

    std::ostringstream tmp;
    Ain->getArrayDesc().getDimensions()[0].toString(tmp) ;
    if(DBG) std::cerr << tmp.str() << std::endl;

    std::ostringstream tmp2;
    Ain->getArrayDesc().getDimensions()[1].toString(tmp2) ;
    if(DBG) std::cerr << tmp2.str() << std::endl;

    // find M,N from input array
    slpp::int_t M = nRow(Ain);
    slpp::int_t N = nCol(Ain);
    if(DBG) std::cerr << "M " << M << " N " << N << std::endl;


    checkInputArray(Ain);
    //
    //.... Set up ScaLAPACK array descriptors ........................................
    //

    // these formulas for LLD (loacal leading dimension) and LTD (local trailing dimension)
    // are found in the headers of the scalapack functions such as pdgesvd_()
        const slpp::int_t MB= chunkRow(Ain);  // chunk sizes
        const slpp::int_t NB= chunkCol(Ain);
    const slpp::int_t one = 1 ;

    // LLD(IN)
    slpp::int_t LLD_IN = std::max(one, scidb_numroc_( M, MB, MYPROW, /*RSRC_IN*/0, NPROW ));
    if(DBG) std::cerr << "M:"<<M <<" MB:"<<MB << " MYPROW:"<<MYPROW << " NPROW:"<<NPROW<< std::endl;
    if(DBG) std::cerr << "--> LLD_IN = " << LLD_IN << std::endl;

    // LLD(OUT)
    slpp::int_t LLD_OUT = LLD_IN; // because its a copy operation

    // LTD(IN)
    slpp::int_t LTD_IN = std::max(one, scidb_numroc_( N, NB, MYPCOL, /*CSRC_IN*/0, NPCOL ));
    if(DBG) std::cerr << "N:"<<N <<" NB:"<<NB  << " MYPCOL:"<<MYPCOL << " NPCOL:"<<NPCOL<< std::endl;
    if(DBG) std::cerr << "-->LTD_IN = " << LTD_IN << std::endl;

    // create ScaLAPACK array descriptors
    slpp::int_t descinitINFO = 0; // an output implemented as non-const ref (due to Fortran calling conventions)
    if(DBG) std::cerr << "scidb_descinit_ DESC_IN" << std::endl;
    slpp::desc_t DESC_IN;
    scidb_descinit_(DESC_IN, M, N, MB, NB, 0, 0, ICTXT, LLD_IN, descinitINFO);
    if (descinitINFO != 0) {
        LOG4CXX_ERROR(logger, "MPICopyPhysical::invokeMPI: scidb_descinit(DESC_IN) failed, INFO " << descinitINFO
                               << " DESC_IN " << DESC_IN);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "MPICopyPhysical::invokeMPI: scidb_descinit(DESC_IN) failed");
    }
    LOG4CXX_DEBUG(logger, "MPICopyPhysical::invokeMPI: DESC_IN " << DESC_IN);

    if(DBG) std::cerr << "scidb_descinit_ DESC_OUT" << std::endl;
    slpp::desc_t DESC_OUT;
    scidb_descinit_(DESC_OUT, M, N, MB, NB, 0, 0, ICTXT, LLD_OUT, descinitINFO);
    if (descinitINFO != 0) {
        LOG4CXX_ERROR(logger, "MPICopyPhysical::invokeMPI: scidb_descinit(DESC_IN) failed, INFO " << descinitINFO
                               << " DESC_OUT " << DESC_OUT);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "MPICopyPhysical::invokeMPI: scidb_descinit(DESC_OUT) failed");
    }
    LOG4CXX_DEBUG(logger, "MPICopyPhysical::invokeMPI: DESC_OUT " << DESC_OUT);

    // create ScaLAPACK array descriptors
    slpp::int_t MP = LLD_IN;
    slpp::int_t NQ = LTD_IN;

    if(DBG) {
        std::cerr << "#### copyMaster#########################################" << std::endl;
        std::cerr << "MB:" << MB << std::endl;
        std::cerr << "NB:" << NB << std::endl;
        std::cerr << "MYPROW:" << MYPROW << std::endl;
        std::cerr << "NPROW:" << NPROW << std::endl;

        std::cerr << "LOCAL SIZES:@@@@@@@@@@@@@@@@@@@" << std::endl ;
        std::cerr << "XX MP   = " << MP << std::endl;
        std::cerr << "XX NQ   = " << NQ << std::endl;
    }

    // local sizes
    const slpp::int_t SIZE_IN = MP * NQ ;
    const slpp::int_t SIZE_OUT = SIZE_IN ;

    const size_t NUM_BUFS=3 ;
    size_t nElem[NUM_BUFS];
    size_t elemBytes[NUM_BUFS];
    std::string dbgNames[NUM_BUFS];

    elemBytes[0] = 1 ; nElem[0] = sizeof(scidb::MPICopyArgs) ; dbgNames[0] = "MPICopyArgs";
    elemBytes[1] = sizeof(double) ; nElem[1] = SIZE_IN ; dbgNames[1] = "IN" ;
    elemBytes[2] = sizeof(double) ; nElem[2] = SIZE_OUT ; dbgNames[2] = "OUT" ;



    //-------------------- Create IPC
    std::vector<MPIPhysical::SMIptr_t> shmIpc = allocateMPISharedMemory(NUM_BUFS, elemBytes, nElem, dbgNames);
    typedef scidb::SharedMemoryPtr<double> shmSharedPtr_t ;

    void *argsBuf = shmIpc[0]->get();
    double* IN = reinterpret_cast<double*>(shmIpc[1]->get());
    double* OUT= reinterpret_cast<double*>(shmIpc[2]->get());
    size_t resultShmIpcIndx = 2;
    shmSharedPtr_t OUTx(shmIpc[resultShmIpcIndx]);

    procRowCol_t firstChunkSize = { chunkRow(Ain), chunkCol(Ain) };
    shared_ptr<PartitioningSchemaDataForScaLAPACK> schemeData =
       make_shared<PartitioningSchemaDataForScaLAPACK>(getBlacsGridSize(inputArrays, query, "MPICopyPhysical"), firstChunkSize);

    shared_ptr<Array> tmpRedistedInput = redistributeInputArray(Ain, schemeData, query, "MPICopyPhysical");

    bool wasConverted = (tmpRedistedInput != Ain) ;  // only when redistribute was actually done (sometimes optimized away)

    setInputMatrixToAlgebraDefault(IN, nElem[1]);
    extractArrayToScaLAPACK(tmpRedistedInput, IN, DESC_IN, NPROW, NPCOL, MYPROW, MYPCOL, query);

    if (wasConverted) {
        SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
        syncArray->sync();
    }
    Ain = tmpRedistedInput;

    setOutputMatrixToAlgebraDefault(OUT, nElem[2], logger);

    // debug that the reformat worked correctly:
    if(DBG) {
        for(int ii=0; ii < SIZE_IN; ii++) {
            std::cerr << "("<< MYPROW << "," << MYPCOL << ") IN["<<ii<<"] = " << IN[ii] << std::endl;
        }
    }

    //
    //.... Call the master wrapper
    //
    LOG4CXX_DEBUG(logger, "MPICopyPhysical::invokeMPI(): calling mpiCopyMaster M,N " << M << "," << N << "MB,NB:" << MB << "," << NB);
    boost::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(_launchId);
    slpp::int_t MYPE = query->getInstanceID() ;  // we map 1-1 between instanceID and MPI rank
    slpp::int_t INFO = DEFAULT_BAD_INFO ;
    mpiCopyMaster(query.get(), _ctx, slave, _ipcName, argsBuf,
                  NPROW, NPCOL, MYPROW, MYPCOL, MYPE,
                  IN,  DESC_IN, OUT, DESC_OUT, INFO);
    raiseIfBadResultInfo(INFO, "mpiCopy");

    boost::shared_array<char> resPtrDummy(reinterpret_cast<char*>(NULL));
    typedef scidb::ReformatFromScalapack<shmSharedPtr_t> reformatOp_t ;

    // Only in MPICopy
    if(DBG) {
        std::cerr << "--------------------------------------" << std::endl;
        std::cerr << "sequential values of 'OUT' ScaLAPACK memory" << std::endl;
        for(int ii=0; ii < SIZE_OUT; ii++) {
            std::cerr << "OUT["<<ii<<"] = " << OUT[ii] << std::endl;
        }
        std::cerr << "--------------------------------------" << std::endl;
        std::cerr << "using pdelgetOp to reformat mpiCopy OUT from memory to scidb array , start" << std::endl ;
    }

    //
    // an OpArray is a SplitArray that is filled on-the-fly by calling the operator
    // so all we have to do is create one with an upper-left corner equal the the
    // global position of the first local block we have.  so we need to map
    // our "processor" coordinate into that position, which we do by multiplying
    // by the chunkSize
    //
    Dimensions const& dims = Ain->getArrayDesc().getDimensions();
    Coordinates first(2);
    first[0] = dims[0].getStartMin() + MYPROW * dims[0].getChunkInterval();
    first[1] = dims[1].getStartMin() + MYPCOL * dims[1].getChunkInterval();

    Coordinates last(2);
    last[0] = dims[0].getStartMin() + dims[0].getLength() - 1;
    last[1] = dims[1].getStartMin() + dims[1].getLength() - 1;

    Coordinates iterDelta(2);
    iterDelta[0] = NPROW * dims[0].getChunkInterval();
    iterDelta[1] = NPCOL * dims[1].getChunkInterval();

    if(DBG) std::cerr << "MPICopy OUT SplitArray from ("<<first[0]<<","<<first[1]<<") to (" << last[0] <<"," <<last[1]<<") delta:"<<iterDelta[0]<<","<<iterDelta[1]<< std::endl;
    LOG4CXX_DEBUG(logger, "MPICopyPhysical::invokeMPI(): Creating array ("<<first[0]<<","<<first[1]<<"), (" << last[0] <<"," <<last[1]<<")");

    reformatOp_t    pdelgetOp(OUTx, DESC_OUT, dims[0].getStartMin(), dims[1].getStartMin());
    shared_ptr<Array> result  = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                                            first, last, iterDelta, query));
    releaseMPISharedMemoryInputs(shmIpc, resultShmIpcIndx);
    unlaunchMPISlaves();

    return result;
}


shared_ptr<Array> MPICopyPhysical::execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
{
    //
    // + converts inputArray[0] to psScaLAPACK distribution
    // + calls invokeMPI()
    // + returns the output OpArray.
    // NOTE: only one input is allowed.
    //
    const bool DBG = false ;

    if(DBG) std::cerr << "MPICopyPhysical::execute() begin ---------------------------------------" << std::endl;

    //
    // repartition and redistribution from SciDB chunks and arbitrary distribution
    // to
    // ScaLAPACK tiles (efficient cache-aware size) and psScaLAPACK, which is
    //   true 2D block-cyclic.  Ideally, we will be able to repart() to from
    //   arbitrary chunkSize to one that is efficient for ScaLAPACK
    //
    size_t nInstances = query->getInstancesCount();
    slpp::int_t instanceID = query->getInstanceID();

    if (inputArrays.size() != 1) {
         throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "MPICopyPhysical must have only one input");
    }

    shared_ptr<Array> input = inputArrays[0];
    Dimensions const& dims = input->getArrayDesc().getDimensions();
    size_t nRows = dims[0].getLength();
    size_t nCols = dims[1].getLength();
    if (!nRows || !nCols ) {
        bool isParticipating = launchMPISlaves(query, 0);
        SCIDB_ASSERT(!isParticipating);

        procRowCol_t firstChunkSize = { chunkRow(inputArrays[0]), chunkCol(inputArrays[0]) };
        shared_ptr<PartitioningSchemaDataForScaLAPACK> schemeData =
        make_shared<PartitioningSchemaDataForScaLAPACK>(getBlacsGridSize(inputArrays, query, "MPICopyPhysical"), firstChunkSize);

        shared_ptr<Array> tmpRedistedInput = redistributeInputArray(inputArrays[0], schemeData, query, "MPICopyPhysical");

        bool wasConverted = (tmpRedistedInput != inputArrays[0]) ;  // only when redistribute was actually done (sometimes optimized away)
        if (wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
        inputArrays[0].reset();
        unlaunchMPISlavesNonParticipating();
        return shared_ptr<Array>(new MemArray(_schema,query));
    }

    //
    //.... Initialize the (imitation)BLACS used by the instances to calculate sizes
    //     AS IF they are MPI processes (which they are not)
    //
    const ProcGrid* procGrid = query->getProcGrid();

    procRowCol_t MN = { nRows, nCols};
    procRowCol_t MNB = { procNum_t(dims[0].getChunkInterval()),
                         procNum_t(dims[1].getChunkInterval()) };

    procRowCol_t blacsGridSize = procGrid->useableGridSize(MN, MNB);
    procRowCol_t myGridPos = procGrid->gridPos(instanceID, blacsGridSize);

    if(DBG) std::cerr << "*** myGridPos.row:" << myGridPos.row << " myGridPos.col:" << myGridPos.col << std::endl;
    if (myGridPos.row >= blacsGridSize.row || myGridPos.col >= blacsGridSize.col) {
        if(DBG) {
            std::cerr << "instID:" << instanceID << " myGridPos.row:" << myGridPos.row
                                                 << " myGridPos.col:" << myGridPos.col << std::endl;
            std::cerr << "NOT in grid: " << blacsGridSize.row << " x " << blacsGridSize.col << std::endl;
            std::cerr << "should not invoke a slave" << std::endl ;
        }
        //
        // We are an "extra" instance that must return an empty array
        // we will not start mpi slaves for such instances
        bool isParticipating = launchMPISlaves(query, 0);
        SCIDB_ASSERT(!isParticipating);

        procRowCol_t firstChunkSize = { chunkRow(inputArrays[0]), chunkCol(inputArrays[0]) };
        shared_ptr<PartitioningSchemaDataForScaLAPACK> schemeData =
        make_shared<PartitioningSchemaDataForScaLAPACK>(getBlacsGridSize(inputArrays, query, "MPICopyPhysical"), firstChunkSize);

        shared_ptr<Array> tmpRedistedInput = redistributeInputArray(inputArrays[0], schemeData, query, "MPICopyPhysical");

        bool wasConverted = (tmpRedistedInput != inputArrays[0]) ;  // only when redistribute was actually done (sometimes optimized away)
        if (wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
        inputArrays[0].reset();
        unlaunchMPISlavesNonParticipating();
        return shared_ptr<Array>(new MemArray(_schema,query));
    } else {
        if(DBG) {
            std::cerr << "instID:" << instanceID << " myGridPos.row:" << myGridPos.row
                                                 << " myGridPos.col:" << myGridPos.col << std::endl;
            std::cerr << "IN GRID: " << blacsGridSize.row << " x " << blacsGridSize.col << std::endl;
        }
    }

    slpp::int_t ICTXT=-1;
    slpp::int_t IC = query->getInstancesCount();
    slpp::int_t NP = blacsGridSize.row * blacsGridSize.col ;

    if(DBG) {
        std::cerr << "(execute) NP:"<<NP << " IC:" <<IC << std::endl;
        std::cerr << "(execute) scidb_set_blacs_gridinfo_(ctx:"<< ICTXT
                                   << ", nprow:"<<blacsGridSize.row
                                   << ", npcol:"<<blacsGridSize.col << "," << std::endl;
        std::cerr << "(execute)                           myRow:" << myGridPos.row
                                   << ", myCol:" << myGridPos.col << ")" << std::endl;
    }
    scidb_set_blacs_gridinfo_(ICTXT, blacsGridSize.row, blacsGridSize.col, myGridPos.row, myGridPos.col);

    // check that it worked
    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    scidb_blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    if(DBG) {
        std::cerr << "scidb_blacs_gridinfo_(ctx:" << ICTXT << ")" << std::endl;
        std::cerr << "   -> gridsiz:(" << NPROW  << ", " << NPCOL << ")" << std::endl;
        std::cerr << "   -> gridPos:(" << MYPROW << ", " << MYPCOL << ")" << std::endl;
    }

    int minLen = std::min(nRows, nCols);
    if(DBG) {
        std::cerr << "-------------------------------------" << std::endl ;
        std::cerr << "MPICopyPhysical::execute(): nInstances=" << nInstances << std::endl ;
        std::cerr << "MPICopyPhysical::execute(): nCols=" << nCols << std::endl ;
        std::cerr << "MPICopyPhysical::execute(): nRows=" << nRows << std::endl ;
        std::cerr << "MPICopyPhysical::execute(): minLen=" << minLen << std::endl ;
        std::cerr << "MPICopyPhysical::execute(): dims[0].getChunkInterval()=" << dims[0].getChunkInterval() << std::endl ;
        std::cerr << "MPICopyPhysical::execute(): dims[1].getChunkInterval()=" << dims[1].getChunkInterval() << std::endl ;
        std::cerr << "-------------------------------------" << std::endl ;

        std::cerr << "MPICopy: preparing to extractData, nRows=" << nRows << ", nCols = " << nCols << std::endl ;
    }

    LOG4CXX_DEBUG(logger, "MPICopyPhysical::execute(): preparing to extractData, nRows=" << nRows << ", nCols = " << nCols);

    Coordinates first(2);
    first[0] = dims[0].getStartMin() + MYPROW * dims[0].getChunkInterval();
    first[1] = dims[1].getStartMin() + MYPCOL * dims[1].getChunkInterval();

    Coordinates last(2);
    last[0] = dims[0].getStartMin() + dims[0].getLength() - 1;
    last[1] = dims[1].getStartMin() + dims[1].getLength() - 1;

    shared_ptr<Array> result = invokeMPI(inputArrays, query, _schema);

    // return the scidb array
    LOG4CXX_DEBUG(logger, "MPICopyPhysical::execute(): returning result");
    Dimensions const& rdims = result->getArrayDesc().getDimensions();

    if(DBG) {
        std::cerr << "returning result array size: " << rdims[0].getLength() <<
                                                 "," << rdims[1].getLength() << std::endl ;
    }
    return result;
}

REGISTER_PHYSICAL_OPERATOR_FACTORY(MPICopyPhysical, "mpicopy", "MPICopyPhysical");

} // namespace
