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

// std C++
#include <cmath>
#include <sstream>
#include <string>

// std C
#include <time.h>

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>

// SciDB
#include <array/StreamArray.h>
#include <array/DelegateArray.h>
#include <array/MemArray.h>
#include <array/OpArray.h>
#include <log4cxx/logger.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include <util/Network.h>
#include <query/Query.h>
#include <system/BlockCyclic.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/shm/SharedMemoryIpc.h>
#include <util/Utility.h>

// MPI/ScaLAPACK
#include <scalapackUtil/reformat.hpp>
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>
#include <dlaScaLA/slaving/pdgemmMaster.hpp>
#include <dlaScaLA/slaving/pdgemmSlave.hpp>

// locals
#include "GEMMOptions.hpp"
#include "DLAErrors.h"


//
// NOTE: code sections marked REFACTOR are being identified as
//       candidates to be moved into MPIOperator and ScaLAPACKOperator
//       base classes.  This is one of the scheduled items for
//       DLA/ScaLAPACK milestone D (Cheshire milestone 4 timeframe)
//

namespace scidb
{
using namespace scidb;
using namespace boost;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra.ops.gemm"));

static const bool DBG_CERR = false;
static const bool DBG_REFORMAT = false;

/**
 *  A Physical multiply operator implemented using ScaLAPACK
 *  The interesting work is done in invokeMPI(), above
 *
 */
class GEMMPhysical : public ScaLAPACKPhysical
{
public:

    GEMMPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :
        ScaLAPACKPhysical(logicalName, physicalName, parameters, schema)
    {
    }
    shared_ptr<Array> invokeMPI(std::vector< shared_ptr<Array> >& inputArrays,
                                const GEMMOptions options, shared_ptr<Query>& query,
                                ArrayDesc& outSchema);

    virtual shared_ptr<Array> execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query);
private:
};


char getTransposeCode(bool transpose) {
    return transpose ? 'T' : 'N' ;
}

shared_ptr<Array> GEMMPhysical::invokeMPI(std::vector< shared_ptr<Array> >& inputArrays,
                                          const GEMMOptions options, shared_ptr<Query>& query,
                                          ArrayDesc& outSchema)
{
    //
    // Everything about the execute() method concerning the MPI execution of the arrays
    // is factored into this method.  This does not include the re-distribution of data
    // chunks into the ScaLAPACK distribution scheme, as the supplied inputArrays
    // must already be in that scheme.
    //
    // + intersects the array chunkGrids with the maximum process grid
    // + sets up the ScaLAPACK grid accordingly and if not participating, return early
    // + start and connect to an MPI slave process
    // + create ScaLAPACK descriptors for the input arrays
    // + convert the inputArrays into in-memory ScaLAPACK layout in shared memory
    // + call a "master" routine that passes the ScaLAPACK operator name, parameters,
    //   and shared memory descriptors to the ScaLAPACK MPI process that will do the
    //   actual computation.
    // + wait for successful completion
    // + construct an "OpArray" that make and Array API view of the output memory.
    // + return that output array.
    //
    enum dummy  {R=0, C=1};              // row column
    enum dummy2 {AA=0, BB, CC, NUM_MATRICES};  // which matrix: alpha AA * BB + beta CC -> result

    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): begin");

    size_t numArray = inputArrays.size();
    if (numArray != NUM_MATRICES) {  // for now ... may make CC optional when beta is 0, later
        LOG4CXX_ERROR(logger, "GEMMPhysical::invokeMPI(): " << numArray << " != NUM_MATRICES " << size_t(NUM_MATRICES));
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << "GEMMPhysical::invokeMPI(): requires 3 input Arrays/matrices.");
    }

    //
    // Initialize the (emulated) BLACS and get the proces grid info
    //
    bool isParticipatingInScaLAPACK = doBlacsInit(inputArrays, query, "GEMMPhysical");
    slpp::int_t ICTXT=-1, NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    scidb_blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    if (isParticipatingInScaLAPACK) {
        checkBlacsInfo(query, ICTXT, NPROW, NPCOL, MYPROW, MYPCOL, "GEMMPhysical");
    }

    LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI() NPROW="<<NPROW<<", NPCOL="<<NPCOL);

    //
    // launch MPISlave if we participate
    // TODO: move this down into the ScaLAPACK code ... something that does
    //       the doBlacsInit, launchMPISlaves, and the check that they agree
    //
    bool isParticipatingInMPI = launchMPISlaves(query, NPROW*NPCOL);
    if (isParticipatingInScaLAPACK != isParticipatingInMPI) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " isParticipatingInScaLAPACK " << isParticipatingInScaLAPACK
                              << " isParticipatingInMPI " << isParticipatingInMPI);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << "GEMMPhysical::invokeMPI(): internal inconsistency in MPI slave launch.");
    }

    if (isParticipatingInMPI) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): participating in MPI");
    } else {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): not participating in MPI");
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): only participating in redistribute of the input");

        // redistribute to psScaLAPACK
        // NOTE: this must be kept in sync with the particpatingInMPI version of the redistribute, below
        // NOTE: this redistribution must be kept in sync with the particpatingInMPI redistributeInputArrays, above
        procRowCol_t firstChunkSize = { chunkRow(inputArrays[0]), chunkCol(inputArrays[0]) };
        shared_ptr<PartitioningSchemaDataForScaLAPACK> schemeData =
           make_shared<PartitioningSchemaDataForScaLAPACK>(getBlacsGridSize(inputArrays, query, "GEMMPhysical"), firstChunkSize);

        for(size_t mat=0; mat < numArray; mat++ ) {
            std::stringstream labelStream;
            labelStream << "GEMMPhysical input[" << mat << "]";
            shared_ptr<Array> tmpRedistedInput = redistributeInputArray(inputArrays[mat], schemeData, query, labelStream.str());
            bool wasConverted = (tmpRedistedInput != inputArrays[mat]) ;  // only when redistribute was actually done (sometimes optimize away)
            if (wasConverted) {
                SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
                syncArray->sync();
            }
            // free potentially large amount of memory, e.g. when inputArrays[mat] was significantly memory-materialized
            inputArrays[mat].reset();

            // TODO: validate that the redistribute brought no chunks to the instance by
            //       getting an array iterator and make sure it returns no chunks
            //       (factor to ScaLAPACKPhysical.cpp)

            // after validating, we don't need tmpRedistedInput anymore, either
            tmpRedistedInput.reset();
        }
        unlaunchMPISlavesNonParticipating();
        return shared_ptr<Array>(new MemArray(_schema,query)); // NOTE: must not happen before redistribute is done.
    }

    //
    // get dimension information about the input arrays
    // TODO: REFACTOR, this is a pattern in DLAs
    //

    // matrix sizes from arrays A,B,C
    matSize_t size[NUM_MATRICES];       // does not change even after redistributeInputArray
    for(size_t i=0; i < numArray; i++ ) {
        size[i] = getMatSize(inputArrays[i]);
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                               << " size["<<i<<"] " << size[i][R] << "," << size[i][C]);

   }



    // TODO JHM : convert 1d arrays to nrows x 1 so we can use vectors as input to GEMM without requiring
    //            the user to add a dimension of size 1.
    for(size_t i=0; i < numArray; i++ ) {
        // TODO JHM : check inputArrays[i] to make sure we are only using 2D arrays,
        //            that may or may not be done by checkInputArrays
        checkInputArray(inputArrays[i]);  // check block size constraints, etc
    }

    //
    //.... Set up ScaLAPACK array descriptors ........................................
    //

    // we would like to do the automatic repart() [not yet implemented] inside the same loop as the
    // redistribute() and extractToScaLAPACK() in order to release each array after it is consumed.
    // unfortunately, we have made some of the routines below dependent on the MB,NB we are going to use,
    // which has recently become determined by the chunkSize of the inputArrays[] since it is no longer
    // a fixed value, but may vary over a legal range.
    // but when automatic repart() is done, we will want to use the chunksize of the output of the repart().
    // so we will need to decide by this point what the MB,NB is going to be, even if we haven't reparted
    // to it yet.
    // to make it clear we mean ScaLAPACK MB,NB
    // (which may become different from the inputArray[i] chunkSize in the future)
    // we will call the array of ScaLAPACK MB,NB pairs,  MB_NB[].
    matSize_t MB_NB[NUM_MATRICES];  // this one should be moved after redistributeInputArrays() for when it really reparts
    for(size_t i=0; i < numArray; i++ ) {
        MB_NB[i] = getMatChunkSize(inputArrays[i]);
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " using MB_NB["<<i<<"] " << MB_NB[i][R] << "," << MB_NB[i][C]);
    }

    // these formulas for LLD (local leading dimension) and LTD (local trailing dimension)
    // are found in the headers of the ScaLAPACK functions such as pdgemm_()
    const slpp::int_t one = 1 ;

    // TODO: turn these pairs into matSize_t matrixLocalSize[NUM_MATRICES];
    slpp::int_t LLD[NUM_MATRICES]; // local leading dimension
    slpp::int_t LTD[NUM_MATRICES]; // local trailing dimension
    for(size_t i=0; i < numArray; i++ ) {
        slpp::int_t RSRC = 0 ;
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " M["<<i<<"][R]"<<size[i][R] <<" MB["<<i<<"][R]:"<<MB_NB[i][R]
                              << " N["<<i<<"][R]"<<size[i][C] <<" NB["<<i<<"][R]:"<<MB_NB[i][C]
                              << " MYPROW:"<<MYPROW << " NPROW:"<< NPROW);
        LLD[i] = std::max(one, scidb_numroc_( size[i][R], MB_NB[i][R], MYPROW, RSRC, NPROW ));
        LTD[i] = std::max(one, scidb_numroc_( size[i][C], MB_NB[i][C], MYPCOL, RSRC, NPCOL ));
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " LLD["<<i<<"] = " << LLD[i]
                              << " LTD["<<i<<"] = " << LTD[i]);
    }

    // create ScaLAPACK array descriptors
    // TODO: lets factor this to a method on ScaLAPACKPhysical
    slpp::desc_t DESC[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " scidb_descinit_(DESC["<<i<<"], M=" << size[i][R] << ", N=" << size[i][C]
                              << ", MB=" << MB_NB[i][R] << ", NB=" << MB_NB[i][R]
                              << ", IRSRC=" << 0 << ", ICSRC=" << 0 << ", ICTXT=" << ICTXT
                              << ", LLD=" << LLD[i]);

        slpp::int_t descinitINFO = 0; // an output implemented as non-const ref (due to Fortran calling conventions)
        scidb_descinit_(DESC[i], size[i][R], size[i][C], MB_NB[i][R], MB_NB[i][C], 0, 0, ICTXT, LLD[i], descinitINFO);
        if (descinitINFO != 0) {
            LOG4CXX_ERROR(logger, "GEMMPhysical::invokeMPI(): scidb_descinit(DESC) failed, INFO " << descinitINFO
                                                                                    << " DESC " << DESC);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << "GEMMPhysical::invokeMPI(): scidb_descinit(DESC) failed");
        }

        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " scidb_descinit_() returned DESC["<<i<<"] " << DESC[i]);

        // debugging for #1986 ... when #instances is prime, process grid is a row.  When small chunk sizes are used,
        // desc.LLD is being set to a number larger than the chunk size ... I don't understand or expect this.
        bool doDebugTicket1986=true;  // remains on until fixed, can't ship with this not understood.
        if(doDebugTicket1986) {
            if (DESC[i].LLD > DESC[i].MB) {
                LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): ticket 1986 issue"
                                      <<  ", DESC["<<i<<"].LLD " << DESC[i].LLD
                                      << " > DESC["<<i<<"].MB: " << DESC[i].MB);
            }
        }
    }

    // matrix allocations are of local size, not global size
    size_t matrixLocalSize[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        matrixLocalSize[i]  = size_t(LLD[i]) * LTD[i] ;
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): " 
                << " LLD[" << i << "] ( " << LLD[i] << " ) x "   
                << " LTD[" << i << "] ( " << LTD[i] << " ) = "  
                << " matrixLocalSize[" << i << "] " << matrixLocalSize[i]);
    }

    //
    // Create IPC buffers
    //
    enum dummy3 {BUF_ARGS=0, BUF_MAT_AA, BUF_MAT_BB, BUF_MAT_CC, NUM_BUFS };
    assert(numArray < NUM_BUFS);

    size_t bufElemBytes[NUM_BUFS];
    size_t bufNumElem[NUM_BUFS];
    std::string bufDbgNames[NUM_BUFS];

    bufElemBytes[BUF_ARGS]= 1 ;           bufNumElem[BUF_ARGS]= sizeof(scidb::PdgemmArgs) ; bufDbgNames[BUF_ARGS] = "PdgemmArgs";
    bufElemBytes[BUF_MAT_AA]= sizeof(double) ; bufNumElem[BUF_MAT_AA]= matrixLocalSize[AA]; bufDbgNames[BUF_MAT_AA] = "A" ;
    bufElemBytes[BUF_MAT_BB]= sizeof(double) ; bufNumElem[BUF_MAT_BB]= matrixLocalSize[BB]; bufDbgNames[BUF_MAT_BB] = "B" ;
    bufElemBytes[BUF_MAT_CC]= sizeof(double) ; bufNumElem[BUF_MAT_CC]= matrixLocalSize[CC]; bufDbgNames[BUF_MAT_CC] = "C" ;
    typedef scidb::SharedMemoryPtr<double> shmSharedPtr_t ;

    for(size_t i=0; i < numArray; i++ ) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): " 
                << " bufElemBytes[" << i << "] = " << bufElemBytes[i]);
    }
    std::vector<MPIPhysical::SMIptr_t> shmIpc = allocateMPISharedMemory(NUM_BUFS, bufElemBytes, bufNumElem, bufDbgNames);

    // the following used to determine the PDGEMM() "K" argument just prior to pdgemm,
    // but now it has to be done before inputArrays[AA] is .reset() in the following loop.
    //
    // Comments on PDGEMM input "K", taken from the netlib PDGEMM argument header:
    // If transa = 'T' or 'C'(true), it is the number of rows in submatrix A."
    // If transa = 'N'(false), it is the number of columns in submatrix A."
    slpp::int_t K = nCol(inputArrays[AA], options.transposeA);

    // the following also used to be done just prior to pdgemm,
    // but now must be done before inputArrays[CC] is .reset() in the following loop.
    // it must also now be a copy, and not a reference, for the same reason.
    Dimensions const dimsCC = inputArrays[CC]->getArrayDesc().getDimensions();

    // now for each input matrix, do the following:
    // 1. redistribute to psScaLAPACK (when not already correct).
    // 2. if actual conversion happened, release the inputArray, which might be a lot of memory, e.g. when inputArray[i] is materialized.
    // 2. zero the ScaLAPACK local block-cyclic storage in shared mem. (so that empty cells will become zeros).
    // 3. extract the (redistributed array) where not-empty, into the ScaLAPACK local matrix memory.
    // 4. release the redistributed array, which might be a lot of memory since SG is currently materializing.
    //
    // The only caller of this routine is the execute() method, and neither the execute() method, nor the executor that calls it,
    // access the inputArrays after calling execute, which is why we can reset() the shared_ptrs to the arrays after consuming the
    // arrrays into the ScaLAPACK memory.
    //
    // redistribute to psScaLAPACK, and convert to ScaLAPACK format.
    // NOTE: this redistribution must be kept in sync with the particpatingInMPI redistributeInputArrays, above

    procRowCol_t firstChunkSize = { chunkRow(inputArrays[0]), chunkCol(inputArrays[0]) };
    shared_ptr<PartitioningSchemaDataForScaLAPACK> schemeData =
       make_shared<PartitioningSchemaDataForScaLAPACK>(getBlacsGridSize(inputArrays, query, "GEMMPhysical"), firstChunkSize);

    double* asDoubles[NUM_MATRICES];
    for(size_t mat=0; mat < numArray; mat++ ) {
        std::stringstream labelStream;
        labelStream << "GEMMPhysical input[" << mat << "]";
        shared_ptr<Array> tmpRedistedInput = redistributeInputArray(inputArrays[mat], schemeData, query, labelStream.str());

        bool wasConverted = (tmpRedistedInput != inputArrays[mat]) ;  // only when redistribute was actually done (sometimes optimize away)

        // TODO would be nice if we could allocate the ScaLAPACK memory after dropping the input array
        //      in case the physical memory for the shmem can be reclaimed from the reset inputArrays[mat]

        size_t buf= mat+1;          // buffer 0 is command buffer, buffers[1..n] correspond to inputs[0..n-1]
        assert(buf < NUM_BUFS);

        asDoubles[mat] = reinterpret_cast<double*>(shmIpc[buf]->get());

        setInputMatrixToAlgebraDefault(asDoubles[mat], bufNumElem[buf]);  // note asDoubles[CC] is input and output to/from ScaLAPACK
        extractArrayToScaLAPACK(tmpRedistedInput, asDoubles[mat], DESC[mat],NPROW, NPCOL, MYPROW, MYPCOL, query);

        if(wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
        // free potentially large amount of memory, e.g. when inputArrays[mat] was significantly memory-materialized
        inputArrays[mat].reset();
        tmpRedistedInput.reset(); // and drop this array before iterating on the loop to the next repart/redist

        if(DBG_REFORMAT) { // that the reformat worked correctly
            for(size_t ii=0; ii < matrixLocalSize[mat]; ii++) {
                LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                                      << " @myPPos("<< MYPROW << "," << MYPCOL << ")"
                                      << " array["<<mat<<"]["<<ii<<"] = " << asDoubles[mat][ii]);
            }
        }
    }
    size_t resultShmIpcIndx = BUF_MAT_CC;           // by default, GEMM assumes it will return something for C
                                                    // but this will change if find we don't particpate in the output
    shmSharedPtr_t Cx(shmIpc[resultShmIpcIndx]);

    //
    //.... Call pdgemm to compute the product of A and B .............................
    //
    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): calling pdgemm_ M,N,K:" << size[AA][R]  << ","
                                                                  << size[BB][R] << ","
                                                                  << size[CC][C]
                           << " MB,NB:" << MB_NB[AA][R] << "," << MB_NB[AA][C]);

    if(DBG_CERR) std::cerr << "GEMMPhysical::invokeMPI(): calling pdgemm to compute" << std:: endl;
    boost::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(_launchId);

    slpp::int_t MYPE = query->getInstanceID() ;  // we map 1-to-1 between instanceID and MPI rank
    slpp::int_t INFO = DEFAULT_BAD_INFO ;
    pdgemmMaster(query.get(), _ctx, slave, _ipcName, shmIpc[BUF_ARGS]->get(),
                  NPROW, NPCOL, MYPROW, MYPCOL, MYPE,
                  getTransposeCode(options.transposeA), getTransposeCode(options.transposeB),
                  size[CC][R], size[CC][C], K,
                  &options.alpha,
                  asDoubles[AA], one, one, DESC[AA],
                  asDoubles[BB], one, one, DESC[BB],
                  &options.beta,
                  asDoubles[CC], one, one, DESC[CC],
                  INFO);
    raiseIfBadResultInfo(INFO, "pdgemm");

    boost::shared_array<char> resPtrDummy(reinterpret_cast<char*>(NULL));
    typedef scidb::ReformatFromScalapack<shmSharedPtr_t> reformatOp_t ;

    if(logger->isTraceEnabled()) {
        LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI():--------------------------------------");
        LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI(): sequential values from 'C' memory");
        for(size_t ii=0; ii < matrixLocalSize[CC]; ii++) {
            LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI(): ("<< MYPROW << "," << MYPCOL << ") C["<<ii<<"] = " << asDoubles[CC][ii]);
        }
        LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI(): --------------------------------------");
        LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI(): using pdelgetOp to reformat Gemm left from memory to scidb array , start");
    }


    //
    // an OpArray is a SplitArray that is filled on-the-fly by calling the operator
    // so all we have to do is create one with an upper-left corner equal to the
    // global position of the first local block we have.  so we need to map
    // our "processor" coordinate into that position, which we do by multiplying
    // by the chunkSize
    //
    Coordinates first(2);
    first[R] = dimsCC[R].getStartMin() + MYPROW * MB_NB[CC][R];
    first[C] = dimsCC[C].getStartMin() + MYPCOL * MB_NB[CC][C];

    Coordinates last(2);
    last[R] = dimsCC[R].getStartMin() + size[CC][R] - 1;
    last[C] = dimsCC[C].getStartMin() + size[CC][C] - 1;

    shared_ptr<Array> result;
    // the process grid may be larger than the size of output in chunks... e.g multiplying A(1x100) * B(100x1) -> C(1x1)
    bool isParticipatingInOutput = first[R] <= last[R] && first[C] <= last[C] ;
    if (isParticipatingInOutput) {
        // there is in fact some output in our shared memory... hook it up to an OpArray
        Coordinates iterDelta(2);
        iterDelta[0] = NPROW * MB_NB[CC][R];
        iterDelta[1] = NPCOL * MB_NB[CC][C];

        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():Creating OpArray from ("<<first[R]<<","<<first[C]<<") to (" << last[R] <<"," <<last[C]<<") delta:"<<iterDelta[R]<<","<<iterDelta[C]);
        reformatOp_t      pdelgetOp(Cx, DESC[CC], dimsCC[R].getStartMin(), dimsCC[C].getStartMin());
        result = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                             first, last, iterDelta, query));
        assert(resultShmIpcIndx == BUF_MAT_CC);
    } else {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): instance participated, but does not output: creating empty MemArray: first ("<<first[R]<<","<<first[C]<<"), last(" << last[R] <<"," <<last[C]<<")");
        result = shared_ptr<Array>(new MemArray(_schema,query));  // same as when we don't participate at all
        resultShmIpcIndx = shmIpc.size();                   // indicate we don't want to hold on to buffer BUF_MAT_CC after all
    }

    // TODO: common pattern in ScaLAPACK operators: factor to base class
    releaseMPISharedMemoryInputs(shmIpc, resultShmIpcIndx);
    unlaunchMPISlaves();

    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI() end");

    return result;
}


shared_ptr<Array> GEMMPhysical::execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
{
    //
    // + converts inputArrays to psScaLAPACK distribution
    // + calls invokeMPI()
    // + returns the output OpArray.
    //
    LOG4CXX_DEBUG(logger, "GEMMPhysical::execute(): begin.");

    // TODO: make a GEMMLogical checkArgs(inputArrays, query); which asserts two or three arrays

    // get string of parameters from the optional 4th argument:
    // (TRANSA, TRANSB, ALPHA, BETA)
    std::string namedOptionStr;
    if (_parameters.size() >= 1) {
        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        typedef boost::shared_ptr<OperatorParamPhysicalExpression> ParamType_t ;
        ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(_parameters[0]);
        assert(paramExpr->isConstant());
        namedOptionStr = paramExpr->getExpression()->evaluate().getString();
    }

    GEMMOptions options(namedOptionStr);

    //
    // invokeMPI()
    //

    // invokeMPI does not manage an empty bitmap yet, but it is specified in _schema.
    // so to make it compatible, we first create a copy of _schema without the empty tag attribute
    Attributes attrsNoEmptyTag = _schema.getAttributes(true /*exclude empty bitmap*/);
    ArrayDesc schemaNoEmptyTag(_schema.getName(), attrsNoEmptyTag, _schema.getDimensions());

    // and now invokeMPI produces an array without empty bitmap except when it is not participating
    shared_ptr<Array> arrayNoEmptyTag = invokeMPI(inputArrays, options, query, schemaNoEmptyTag);


    // now we place a wrapper array around arrayNoEmptyTag, that adds a fake emptyTag (true everywhere)
    // but otherwise passes through requests for iterators on the other attributes.
    // And yes, the class name is the complete opposite of what it shold be.
    shared_ptr<Array> result;
    if (arrayNoEmptyTag->getArrayDesc().getEmptyBitmapAttribute() == NULL) {
        result = make_shared<NonEmptyableArray>(arrayNoEmptyTag);
    } else {
        result = arrayNoEmptyTag;
    }

    // return the scidb array
    LOG4CXX_DEBUG(logger, "GEMMPhysical::execute(): (successful) end");
    return result;
}

REGISTER_PHYSICAL_OPERATOR_FACTORY(GEMMPhysical, "gemm", "GEMMPhysical");

} // namespace
