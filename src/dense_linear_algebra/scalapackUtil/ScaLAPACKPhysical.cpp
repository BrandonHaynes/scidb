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
///
/// ScaLAPACKPhysical.cpp
///
///
// std C++
#include <sstream>
#include <string>

// std C
#include <assert.h>

// de-facto standards
#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>

// SciDB
#include <system/BlockCyclic.h>
#include <system/Exceptions.h>
#include <system/Utils.h>


// more SciDB
#include <array/ArrayExtractOp.hpp>
#include <array/OpArray.h>
#include <DLAErrors.h>
#include <mpi/MPIPhysical.hpp>
#include <scalapackUtil/reformat.hpp>
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>
#include <dlaScaLA/slaving/pdgesvdMaster.hpp>
#include <dlaScaLA/slaving/pdgesvdSlave.hpp>
#include <scalapackUtil/reformat.hpp>

#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include <system/Cluster.h>

namespace scidb {
static const bool DBG = false;
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.scalapack.physicalOp"));


// TODO: REFACTORING: continue use of matSize_t in more places
// TODO: REFACTORING: make a "super array" that encapsulates the SciDB::Array and the ScaLAPACK DESC
//                    so we can pass fewer arguments


void checkBlacsInfo(shared_ptr<Query>& query, slpp::int_t ICTXT,
                    slpp::int_t NPROW, slpp::int_t NPCOL, slpp::int_t MYPROW, slpp::int_t MYPCOL,
                    const std::string& callerLabel)
{
    const size_t nInstances = query->getInstancesCount();
    slpp::int_t instanceID = query->getInstanceID();

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::checkBlacsInfo()"
                           << " (via " << callerLabel << "):"
                           << " checkBlacsInfo(ctx " << ICTXT << ") start"
                           << " NPROW " << NPROW  << ", NPCOL " << NPCOL << ")"
                           << " ; MYPROW " << MYPROW << ", MYPCOL" << MYPCOL << ")");

    // REFACTOR these checks
    if(MYPROW < 0 || MYPCOL < 0) {
        LOG4CXX_ERROR(logger, "ScaLAPACKPhysical::checkBlacsInfo():"
                                << " via " << callerLabel
                                << " zero size mpi process grid: MYPROW " << MYPROW
                                << " MYPCOL " << MYPCOL);
                        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                                   << "ScaLAPACKPhysical::checkBlacsInfo(): zero size mpi process grid");
    }

    if(MYPROW >= NPROW) {
        LOG4CXX_ERROR(logger, "ScaLAPACKPhysical::checkBlacsInfo():"
                                << " via " << callerLabel
                                << " MYPROW " << MYPROW << " >= NPROW " << NPROW);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "ScaLAPACKPhysical::checkBlacsInfo(): illegal position in mpi process grid");
    }

    if(MYPCOL >= NPCOL) {
        LOG4CXX_ERROR(logger, "ScaLAPACKPhysical::checkBlacsInfo():"
                                    << " via " << callerLabel
                                    << " MYPCOL " << MYPCOL << " >= NPCOL " << NPCOL);
                            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                                   << "ScaLAPACKPhysical::checkBlacsInfo(): illegal position in mpi process grid");
    }

    // check that mpi_commsize(NPE, MYPE) values
    // which are managed in the slave as:
    //     NPE = MpiManager::getInstance()->getWorldSize();
    //     MYPE = MpiManager::getInstance()->getRank();
    // and here can be derived from the blacs_getinfo
    //
    // lets check them against the instanceCount and instanceID to make sure
    // everything is consistent

    // NPE <= instanceCount
    size_t NPE = NPROW*NPCOL; // from blacs
    if(NPE > nInstances) {
        std::stringstream msg; msg << "Scalapack operator error: NPE "<<NPE<< " nInstances "<< nInstances;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << msg.str()) ;
    }

    // MYPE == instanceID
    slpp::int_t MYPE = MYPROW*NPCOL + MYPCOL ; // row-major
    if(MYPE != instanceID) {
        std::stringstream msg; msg << "Scalapack operator error: MYPE "<<MYPE<< " instanceID "<< instanceID;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << msg.str()) ;
    }

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::checkBlacsInfo"
                           << " via " << callerLabel
                           << " NPE/nInstances " << NPE
                           << " MYPE/instanceID " << MYPE);
}

void ScaLAPACKPhysical::checkInputArray(boost::shared_ptr<Array>& Ain) const
{
    //
    // ScaLAPACK computation routines are only efficient for a certain
    // range of sizes and are generally only implemented for
    // square block sizes.  Check these constraints
    //
    // TODO JHM : rename Ain -> array
    // chunksize was already checked in ScaLAPACKLogical.cpp, but since this code
    // was already here, we'll just fix it to check the same limit, rather than
    // remove it this late in the 12.11 release.
    // TODO: resolve better


    const slpp::int_t MB= chunkRow(Ain);
    const slpp::int_t NB= chunkCol(Ain);

    // TODO JHM: add test case for illegitimate block size
    // TODO JHM test early, add separate auto repart in execute if not efficient size, then retest
    if (MB > slpp::SCALAPACK_MAX_BLOCK_SIZE ||
        NB > slpp::SCALAPACK_MAX_BLOCK_SIZE) {
        std::stringstream ss; ss << "ScaLAPACK operator error:"
                                 << " chunksize "    << chunkRow(Ain)
                                 << " or chunksize " << chunkCol(Ain)
                                 << " is too large."
                                 << " Must be " << slpp::SCALAPACK_MIN_BLOCK_SIZE
                                 << " to "      << slpp::SCALAPACK_MAX_BLOCK_SIZE ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << ss.str());
    }

    if (MB != NB) {
        std::stringstream ss; ss << "ScaLAPACK operator error: row chunksize " << chunkRow(Ain)
                                                    << " != column chunksize "<< chunkCol(Ain)
                                                    << " which is required." ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << ss.str());
    }
}


void extractArrayToScaLAPACK(boost::shared_ptr<Array>& array, double* dst, slpp::desc_t& desc,
                             slpp::int_t nPRow, slpp::int_t nPCol,
                             slpp::int_t myPRow, slpp::int_t myPCol, const shared_ptr<Query>& query)
{
    // use extractDataToOp() and the reformatToScalapack() operator
    // to reformat the data according to ScaLAPACK requirements.
    Coordinates coordFirst = getStartMin(array.get());
    Coordinates coordLast = getEndMax(array.get());
    scidb::ReformatToScalapack pdelsetOp(dst, desc, coordFirst[0], coordFirst[1], nPRow, nPCol, myPRow, myPCol);

    Timing reformatTimer;
    extractDataToOp(array, /*attrID*/0, coordFirst, coordLast, pdelsetOp, query);
    LOG4CXX_DEBUG(logger, "extractArrayToScaLAPACK took " << reformatTimer.stop());
}

bool ScaLAPACKPhysical::requiresRepart(ArrayDesc const& inputSchema) const
{
    return false ; // see #2032 ... have to disable as it will core dump otherwise

    Dimensions const& inDims = inputSchema.getDimensions();

    for(size_t dimIdx=0; dimIdx < inDims.size(); dimIdx++) {
        if(inDims[dimIdx].getChunkInterval() != slpp::SCALAPACK_EFFICIENT_BLOCK_SIZE) {
            return true;
        }
    }

    // #2032
    // (A)     the optimizer won't insert on all inputs, rumour is, so we raise exceptions in the
    //     logical::inferSchemas() until this is fixed.  If I try it, it faults anyway,
    //     so waiting on an answer to #2032 before moving forward
    // (B)     The test above returns true more than we want to, because we can't compare and
    //     analyze the differing chunksizes to determine which we will change and which one
    //     will be the common one we change all the others to.
    //         Since we can't analyze them, we have to insist they are a specific size, when
    //     the user may know well what they are doing and using one in the acceptable range.
    //         Proposal ... requiresRepart() passes in the inputSchemas for all inputs
    //     and returns a vector of bool, or specifies which it is asking about, and we answer with that
    //     single bool.
    return false;
}

ArrayDesc ScaLAPACKPhysical::getRepartSchema(ArrayDesc const& inputSchema) const
{
    Dimensions const& inDims = inputSchema.getDimensions();

    Dimensions resultDims;
    for (size_t dimIdx =0; dimIdx < inDims.size(); dimIdx++)
    {
        DimensionDesc inDim = inDims[dimIdx];
        resultDims.push_back(DimensionDesc(inDim.getBaseName(),
                              inDim.getNamesAndAliases(),
                              inDim.getStartMin(),
                              inDim.getCurrStart(),
                              inDim.getCurrEnd(),
                              inDim.getEndMax(),
                              slpp::SCALAPACK_EFFICIENT_BLOCK_SIZE,  // no way to generate a consensus size.
                              0));
    }

    Attributes inAttrs = inputSchema.getAttributes();
    return ArrayDesc(inputSchema.getName(), inAttrs, resultDims);
}

shared_ptr<Array> ScaLAPACKPhysical::redistributeOutputArrayForTiming(shared_ptr<Array>& outputArray, shared_ptr<Query>& query, const std::string& callerLabel)
{
    // NOTE: for timing only.  Normally, the query planner inserts the redistribute
    // between ScaLAPACK-based operators which have output type psScaLAPACK,
    // and other operators, such as store, that require, e.g. type psHashPartitioned
    // but until the "consume()" operator is completed, and can request redistribution to RR,
    // I need a way to force in the redistribution to measure its cost, when the terminal operator
    // is filter(val > 1e200), which is my workaround for not having "consume"

    // redistribute back to psHashPartitioned
    shared_ptr<Array>redistOutput = redistributeToRandomAccess(outputArray, query, psHashPartitioned,
                                                               ALL_INSTANCE_MASK,
                                                               shared_ptr<DistributionMapper>(),
                                                               0,
                                                               shared_ptr<PartitioningSchemaData>());
    return redistOutput;
}

///
/// convert a set of inputArrays to psScaLAPACK distribution.  Doing them as a set allows certain extra sanity checks,
/// but is not efficient use of memory, so this version is being phased out, or changed to only do the checks.
///
std::vector<shared_ptr<Array> > ScaLAPACKPhysical::redistributeInputArrays(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query>& query, const std::string& callerLabel)
{
    //
    // + converts a set of inputArrays to psScaLAPACK distribution
    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArrays(): via " << callerLabel << " begin.");

    std::vector<shared_ptr<Array> > result;

    // redistribute to psScaLAPACK
    procRowCol_t firstChunkSize = { chunkRow(inputArrays[0]), chunkCol(inputArrays[0]) };
    shared_ptr<PartitioningSchemaDataForScaLAPACK> schemeData =
       make_shared<PartitioningSchemaDataForScaLAPACK>(getBlacsGridSize(inputArrays, query, callerLabel), firstChunkSize);

    for(size_t ii=0; ii < inputArrays.size(); ii++) {
        if (inputArrays[ii]->getArrayDesc().getPartitioningSchema() != psScaLAPACK) {
            // when automatic repartitioning is introduced, have to decide which of the chunksizes will be the target.
            // Until then, we assert they all are the same (already checked in each Logical operator)
            assert(chunkRow(inputArrays[ii]) == firstChunkSize.row &&
                   chunkCol(inputArrays[ii]) == firstChunkSize.col );

            result.push_back(redistributeInputArray(inputArrays[ii], schemeData, query, callerLabel));
        }
    }

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArrays(): via " << callerLabel << " end.");
    return result;
}

/// convert a single inputArray to psScaLAPACK distribution.  fewer sanity checks can be done in this case.
/// So what is the motivation for this method?
/// Alternating redistribute and extractToScaLAPACK allows the input array and redistributed array to be released before
/// any more inputs are processed.  This reduces the memory overhead in gemm(), which uses up to 3 inputs(), considerably.
/// and allows mem-array-threshold to be set higher for the same amount of total system memory.
shared_ptr<Array> ScaLAPACKPhysical::redistributeInputArray(shared_ptr<Array>& inputArray,
                                                            const shared_ptr<PartitioningSchemaDataForScaLAPACK>& schemeData,
                                                            shared_ptr<Query>& query, const std::string& callerLabel)
{
    assert(schemeData);
    //
    // + converts a single inputArrays to psScaLAPACK distribution
    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArray: via " << callerLabel << " begin.");

    // repartition and redistribute from SciDB chunks and arbitrary distribution
    // to ScaLAPACK-sized chunks on the SciDB instance that corresponds to the correct
    // ScaLAPACK process in the ScaLAPACK process grid.
    // Right now, this is just the redistribute, but at some point will include automatic
    // repart() as well, as soon as repart() is fast enough use in practice.
    // (right now, it is too expensive, and instead it is advisable to use a chunksize of
    // of 1000 or 1024 (square), which gives acceptable performance on the SciDB side,
    // at a 5-15% extra cost to the m^3 portion.
    //

    shared_ptr<Array> result = inputArray ; // in case no processing needed, the output is the input

    const size_t nInstances = query->getInstancesCount();
    bool requiresRedistribute = true ;  // TODO: when bringup is done, can set this false, but
                                        //       its possible the 1-instance optimization below is already
                                        //       contained inside redistribute, and can be removed from here.
                                        // TODO: should test the above.

    if (nInstances>1 || requiresRedistribute) {
#if 0
        // TODO: listed in ticket #1962, we do not yet handle chunksizes above some fixed limit by introducing a repart
        if (chunking is not square or not within limits or for some other reason needs repart. ) {
            // repart in addition to redistributing
            result=redistribute(repartArray(input), query, psScaLAPACK);
        } else
#endif
        {
            // redistribute to psScaLAPACK
            if (inputArray->getArrayDesc().getPartitioningSchema() != psScaLAPACK) {
                // redistribute is needed
                Timing redistTime;

                // do the redistribute
                result=pullRedistribute(inputArray, query, psScaLAPACK, ALL_INSTANCE_MASK,
                                               boost::shared_ptr<DistributionMapper>(), /*shift*/0, schemeData);
                LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArray: redistribute() took " << redistTime.stop() << " via " << callerLabel);
                LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArray:"
                                       << " via " << callerLabel
                                       << " chunksize (" << inputArray->getArrayDesc().getDimensions()[0].getChunkInterval()
                                       << ", "           << inputArray->getArrayDesc().getDimensions()[1].getChunkInterval()
                                       << ")");
            } else {
                LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArray: redistribute() took " << 0 << " (skipped) via " << callerLabel);
                LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArray:"
                                       << " via " << callerLabel
                                       << " chunksize (" << inputArray->getArrayDesc().getDimensions()[0].getChunkInterval()
                                       << ", "           << inputArray->getArrayDesc().getDimensions()[1].getChunkInterval()
                                       << ")");
            }
        }
    } else {
        LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArray: redistribute() took " << 0 << " (skipped) via " << callerLabel);
        LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArray:"
                               << " via " << callerLabel
                               << " single instance -> no redist needed.");
    }

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArray: via " << callerLabel << " end");
    return result;
}

bool ScaLAPACKPhysical::doBlacsInit(std::vector< shared_ptr<Array> >& redistInputs, shared_ptr<Query>& query, const std::string& callerLabel)
{
    //
    //.... Initialize the (imitation)BLACS used by the instances to calculate sizes
    //     AS IF they are MPI processes (which they are not).  But the API is as if we were
    //     actually going to do the ScaLAPACK in-process.  (This is important because we may well
    //     port the BLACS directly into SciDB and have the option of skipping the MPI layer
    //     altogether.  This will work only for ScaLAPACK which has this additional portability layer
    //     most modern numeric codes are coded directly to MPI, so it is still extremely useful that
    //     we built the MPI layer.)
    //
    // + get the size of the blacs grid we are going to use
    // + get our position in the grid
    // + sets up the ScaLAPACK grid accordingly and if not participating, return early
    //
    bool result(true);
    // get size of the grid we are going to use
    procRowCol_t blacsGridSize = getBlacsGridSize(redistInputs, query, callerLabel);

    slpp::int_t instanceID = query->getInstanceID();
    const ProcGrid* procGrid = query->getProcGrid();
    procRowCol_t myGridPos = procGrid->gridPos(instanceID, blacsGridSize);

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit():"
                              << " via " << callerLabel
                              << " gridPos (" << myGridPos.row << ", " << myGridPos.col << ")"
                              << " gridSize (" << blacsGridSize.row << ", " << blacsGridSize.col << ")");

    if (myGridPos.row >= blacsGridSize.row || myGridPos.col >= blacsGridSize.col) {
        LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit():"
                                  << " via " << callerLabel
                                  << " instance " << instanceID << " NOT in grid"
                                  << " gridPos (" << myGridPos.row << ", " << myGridPos.col << ")"
                                  << " gridSize (" << blacsGridSize.row << ", " << blacsGridSize.col << ")");
        //
        // We are an "extra" instance that must return an empty array
        // we won't start mpi slaves for such instances

        // XXX Make sure that the coordinator always participates
        // to work around the apparent (OPENMPI) mpirun bug in dealing with --prefix,
        // where the --prefix specified for the first instance overrides all the following ones.
        // As long as the coordinator=0, the condition should be true.
        // XXX TODO: fix it for any coordinator,
        // BUT we are not using OPENMPI (now anyway) !
        // assert(!query->isCoordinator());
        LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit():"
                                   << " via " << callerLabel
                                   << " instID " << instanceID << "not in grid, returning false, fake BLACS not initialized.");
        result = false ;
    } else {
        LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit():"
                                   << " via " << callerLabel
                                   << " instID " << instanceID << "is in grid.");
    }

    slpp::int_t ICTXT=-1;

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit():"
                              << " via " << callerLabel
                              << " calling scidb_set_blacs_gridinfo_(ctx " << ICTXT
                              << ", nProw " << blacsGridSize.row << ", nPcol "<< blacsGridSize.col
                              << ", myPRow " << myGridPos.row << ", myPCol " << myGridPos.col << ")");
    scidb_set_blacs_gridinfo_(ICTXT, blacsGridSize.row, blacsGridSize.col, myGridPos.row, myGridPos.col);

    // check that it worked
    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    scidb_blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit():"
                              << " via " << callerLabel
                              << " scidb_blacs_gridinfo(" << ICTXT << ") returns "
                              << " gridsiz (" << NPROW  << ", " << NPCOL << ")"
                              << " gridPos (" << MYPROW << ", " << MYPCOL << ")");

    return result;
}


procRowCol_t ScaLAPACKPhysical::getBlacsGridSize(std::vector< shared_ptr<Array> >& redistInputs, shared_ptr<Query>& query, const std::string& callerLabel)
{
    // find max (union) size of all array/matrices.  this works for most ScaLAPACK operators
    size_t maxSize[2];
    maxSize[0] = 0;
    maxSize[1] = 0;
    BOOST_FOREACH( shared_ptr<Array> input, redistInputs ) {
        matSize_t inputSize = getMatSize(input);
        maxSize[0] = std::max(maxSize[0], inputSize[0]);  // add max() operator to matSize_t?
        maxSize[1] = std::max(maxSize[1], inputSize[1]);
    }
    if (!maxSize[0] || !maxSize[1] ) {
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_OPERATOR, DLA_ERROR7);
    }

    // special cases needed by some operators:
    switch(_gridRule) {
    case RuleNotHigherThanWide:
        // grid height must not exceed grid width (the converse is permitted)
        if ( maxSize[0] > maxSize[1]) {
            maxSize[0] = maxSize[1];
        }
        assert(maxSize[0] <= maxSize[1]); // nrow never greater than ncol
    case RuleInputUnion:
        break; // the union case is handled before this block
    default:
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "no such GridSize rule");
        break; // NOTREACHED
    }

    const ProcGrid* procGrid = query->getProcGrid();
    procRowCol_t MN = { maxSize[0], maxSize[1]};
    procRowCol_t MNB = { chunkRow(redistInputs[0]), chunkCol(redistInputs[0]) };
    // TODO: when automatic repartitioning is introduced, have to decide which of the
    //       chunksizes will be the target chunksize, MNB
    //       Right now, we assert they were the same (presently checked in each Logical operator)
    BOOST_FOREACH( shared_ptr<Array> input, redistInputs ) {
        assert(chunkRow(input) == MNB.row && chunkCol(input) == MNB.col);
    }

    return  procGrid->useableGridSize(MN, MNB);
}


void ScaLAPACKPhysical::raiseIfBadResultInfo(slpp::int_t INFO, const std::string& operatorName) const
{
    // a standard way to raise an error when a pTXXXXXMaster() routine returns
    // non-zero INFO from the corresponding    pTXXXXX_() call in the slave.
    // INFO is the INFO value returned from the fortran slave program
    // operatorName = pTXXXXX , for example pdgemm or pdgesvd
    if (INFO != 0) {
        std::stringstream logMsg;
        if (INFO < 0) {
            logMsg << "error at argument " << -INFO ;
        } else {
            logMsg << "runtime error " << INFO ;
        }
        LOG4CXX_ERROR(logger, "ScaLAPACKPhysical::raiseIfBadResultInfo(): slaved " << operatorName << "() " << logMsg.str());

        std::stringstream exceptionMsg;
        exceptionMsg << operatorName << "() " << logMsg.str();
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << exceptionMsg.str());
    }
}



} // namespace



