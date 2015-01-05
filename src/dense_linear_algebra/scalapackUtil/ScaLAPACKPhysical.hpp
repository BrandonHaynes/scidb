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
/// ScaLAPACKPhysical.hpp
///
///

#ifndef SCALAPACKPHYSICAL_HPP_
#define SCALAPACKPHYSICAL_HPP_

// std C++
#include <cstring>
#include <tr1/array>

// std C

// de-facto standards

// SciDB
#include <log4cxx/logger.h>
#include <query/Query.h>

// MPI/ScaLAPACK
#include <mpi/MPIPhysical.hpp>                    // NOTE: there are many handy helpers in this lower level, worth perusing
#include <scalapackUtil/dimUtil.hpp>
// local
#include "scalapackFromCpp.hpp"   // TODO JHM : rename slpp::int_t


static log4cxx::LoggerPtr SCALAPACKPHYSICAL_HPP_logger(log4cxx::Logger::getLogger("scidb.scalapack.physical.op.hpp"));

namespace scidb {

// handy inline, divide, but if there is a remainder, go to the next higher number
// e.g. the number of blocks/groups of size divisor required to hold val units total.
template<typename int_tt>
inline int_tt divCeil(int_tt val, int_tt divisor) {
    return (val + divisor - 1) / divisor ;
}

// handy inline, round up to a multiple of factor
template<typename int_tt>
inline int_tt roundUp(int_tt val, int_tt factor) {
    return divCeil(val, factor) * factor ;
}

/// call with twice the length for complex or complex-double
/// rather than setting real_tt to a struct
/// we'll worry about specializing to memset() or bzero() for integer
/// types at a later time.
template<class val_tt>
void valsSet(val_tt* dst, val_tt val, size_t numVal) {
    // trivially unrollable
    for (size_t jj=0; jj < numVal; jj++) {
        dst[jj] = val;
    }
}

template<class val_tt>
bool bufferTooLargeForScalapack(size_t numElem) {
    // platform BLAS, LAPACK, SCALAPACK are using 32-bit fortran INTEGER, reflected in slpp::int_t
    return ssize_t(sizeof(val_tt)*numElem) > std::numeric_limits<slpp::int_t>::max() ;   // > 2 Gibibyte
}

template<class float_tt>
void setInputMatrixToAlgebraDefault(float_tt* dst, size_t numVal) {
    Timing timer;
    memset(dst, 0, sizeof(float_tt) * numVal); // empty cells are implicit zeros for sparse matrices

    enum dummy {DBG_DENSE_ALGEBRA_WITH_NAN_FILL=0};  // won't be correct if empty cells present
    if(DBG_DENSE_ALGEBRA_WITH_NAN_FILL) {
        valsSet(dst, ::nan(""), numVal); // any non-signalling nan will do
        LOG4CXX_WARN(SCALAPACKPHYSICAL_HPP_logger, "@@@@@@@@@@@@@ WARNING: prefill matrix memory with NaN for debug");
    }
    LOG4CXX_DEBUG(SCALAPACKPHYSICAL_HPP_logger, "setInputMatrixToAlgebraDefault took " << timer.stop());
}

template<class float_tt>
void setOutputMatrixToAlgebraDefault(float_tt* dst, size_t numVal, log4cxx::LoggerPtr logger) {
    Timing timer;
    valsSet(dst, ::nan(""), numVal); // ScaLAPACK algorithm should provide all entries in matrix
    LOG4CXX_DEBUG(SCALAPACKPHYSICAL_HPP_logger, "setOutputMatrixToAlgebraDefault took " << timer.stop());
}

void checkBlacsInfo(shared_ptr<Query>& query, slpp::int_t ICTXT,
                    slpp::int_t NPROW, slpp::int_t NPCOL,
                    slpp::int_t MYPROW, slpp::int_t MYPCOL,
                    const std::string& callerLabel) ;

///
/// ScaLAPACK computation routines are only efficient for a certain
/// range of sizes and are generally only implemented for
/// square block sizes.  Check these constraints
///
void extractArrayToScaLAPACK(boost::shared_ptr<Array>& array, double* dst, slpp::desc_t& desc,
                             slpp::int_t nPRow, slpp::int_t nPCol,
                             slpp::int_t myPRow, slpp::int_t myPCol, const shared_ptr<Query>& query);

class ScaLAPACKPhysical : public MPIPhysical
{
public:
    static const slpp::int_t DEFAULT_BAD_INFO = -99;                 // scalapack negative errors are the position of the bad argument

    /**
     * @see     MPIPhysical::MPIPhysical
     *
     * @param rule                  certain operators have constraints on the shape of their processor grid
     */
    enum GridSizeRule_e             {RuleInputUnion=0, RuleNotHigherThanWide};
    ScaLAPACKPhysical(const std::string& logicalName, const std::string& physicalName,
                      const Parameters& parameters, const ArrayDesc& schema,
                      GridSizeRule_e gridRule=RuleInputUnion)
    :
        MPIPhysical(logicalName, physicalName, parameters, schema),
        _gridRule(gridRule)
    {
    }

    // standard API
    virtual bool                    changesDistribution(const std::vector<ArrayDesc> & inputSchemas) const
                                    { return true; }

    virtual ArrayDistribution       getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                          const std::vector< ArrayDesc> & inputSchemas) const
                                    { return ArrayDistribution(psScaLAPACK); }

    virtual bool                    requiresRepart(ArrayDesc const& inputSchema) const;
    virtual ArrayDesc               getRepartSchema(ArrayDesc const& inputSchema) const;

    // extending API

    /**
     * for timing only.
     * For proper operation, the query planner inserts the redistribute
     * between ScaLAPACK-based operators (which have distribution psScaLAPACK) and others
     * (e.g. store) which require psHashPartitioned.  However, this requires using store() to
     * as the terminal operator, which induces very long IO wait time into the execution of
     * the benchmark.  Donghui is learning how to use the sg() operator to do this in such a
     * benchmark situation, by writing the AFL    sg(op-under-test, ...) but so far does not understand
     * all the options to sg().  Until that time, I'm just providing a function that can optionally
     * be called within the scalapack operators under control of an environment variable, and this
     * code can be dropped once we switch to sg(op-under-test, ...)
     *
     * @param outputArray   What would have been the output array, without this optional redistribute
     * @param query         Current query
     * @param callerLabel   a string that can be used when labeling logging messages, since the context in which
     *                      this routine is working would be significant to the log line.
     * @return              outputArray redistributed to psHashPartitioned.
     */
    shared_ptr<Array> redistributeOutputArrayForTiming(shared_ptr<Array>& outputArray, shared_ptr<Query>& query, const std::string& callerLabel);

    /**
     * Make the provided inputArrays conform to the general requirements of ScaLAPACK operators, e.g.
     * that they have acceptable chunk size and distribution.
     *
     * NOTE: the automatic repart() is not implemented yet, but we located inside this method.
     * NOTE: at some point this may also include extractToScaLAPACK, so that it completely processes a single
     *       input into ScaLAPACK memory, at which point a multi-input version will be acceptable again.
     *
     * @param inputArrays   The input arrays provided to the physical operator.
     * @param query         current query
     * @param callerLabel   a string that can be used when labeling logging messages, since the context in which
     *                      this routine is working would be significant to the log line.
     * @return              transformed or passed-through inputArrays, as appropriate
     */
    std::vector<shared_ptr<Array> > redistributeInputArrays(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query>& query, const std::string& callerLabel);

    /**
     * Make the provided inputArray conform to the general requirements of ScaLAPACK operators, e.g.
     * that they have acceptable chunk size and distribution.
     * NOTE: the automatic repart() is not implemented yet, but may well go inside this method, which would then
     *       be renamed to repartAndRedistInputArrays().
     * NOTE: at some point this may be combined with extraction, so that it completely processes a single
     *       input into ScaLAPACK memory, at which point a multi-input version will be acceptable again.
     * NOTE: this single-array version is needed for use inside operators like gemm() which have multiple inputs to redistribute
     *       and must extractToScaLAPACK and reset() the shared pointer to the input array afterwards, in order to conserve
     *       memory vs converting multiple arrays together as in the method above.  Perhaps with a little more careful
     *       rework, the reset() can be done safely inside the redistributeInputArrays() routine, but right now for backward
     *       compatibility it does not.
     *
     * @param inputArray    The input array provided to the physical operator.
     * @param schemData     Information about the ScaLAPACK process grid chosen for the operator as a whole.
     *                      See redistributeInputArrays() for an example of constructing it.
     * @param query         current query
     * @param callerLabel   a string that can be used when labeling logging messages, since the context in which
     *                      this routine is significant when reading the log line.
     * @return              transformed or passed-through inputArray, as appropriate
     */
    shared_ptr<Array> redistributeInputArray(shared_ptr<Array>& inputArray,
                                             const shared_ptr<PartitioningSchemaDataForScaLAPACK>& schemeData,
                                             shared_ptr<Query>& query, const std::string& callerLabel);

    /**
     * Initialize the ScaLAPACK BLACS (Basic Linear Algebra Communications Systems).
     * @param redistInputs  The final inputs to the operator (already repartitioned and redistributed
     * @param query         Current query
     * @return              Whether the instance participates in the ScaLAPACK computation or may instead
     */
    bool                            doBlacsInit(std::vector< shared_ptr<Array> >& redistributedInputs, shared_ptr<Query>& query, const std::string& callerLabel);

    /**
     * compute the correct ScaLAPACK BLACS process grid size for a particular set of input Arrays (Matrices)
     * @param redistributedInputs   the matrices
     * @param query                 current query
     * @param callerLabel           identify the context of the call, essential for nested operator debugging
     * @return                      the BLACS grid size
     */
    virtual procRowCol_t            getBlacsGridSize(std::vector< shared_ptr<Array> >& redistributedInputs, shared_ptr<Query>& query, const std::string& callerLabel);

    /**
     * a standard way to test INFO returned by a slave,
     * logging and raising any error in a standardized way.
     * @param INFO          the ScaLAPACK INFO value as returned by MPISlaveProxy::waitForStatus()
     * @param operatorName  the ScaLAPACK operator name, e.g "pdgemm" or "pdgesvd" (do not include "Master" suffix)
     */
    void                            raiseIfBadResultInfo(slpp::int_t INFO, const std::string& operatorName) const ;

protected:
    /// routines that make dealing with matrix parameters
    /// more readable and less error prone

    /// a structure to retrieve matrix parameters as a short vector -> 1/2 as many LOC as above
    /// very handy for the operators
    typedef std::tr1::array<size_t, 2 > matSize_t;
    /// get matrix size as vector
    matSize_t getMatSize(boost::shared_ptr<Array>& array) const;
    /// get matrix chunk size as vector
    matSize_t getMatChunkSize(boost::shared_ptr<Array>& array) const;

    void checkInputArray(boost::shared_ptr<Array>& Ain) const ;

private:
    GridSizeRule_e          _gridRule;  // some operators need special rules for determining the
                                        // best way to map their matrices to the processor grid
};


inline ScaLAPACKPhysical::matSize_t ScaLAPACKPhysical::getMatSize(boost::shared_ptr<Array>& array) const
{
    assert(array->getArrayDesc().getDimensions().size() == 2);

    matSize_t result;
    result.at(0) = array->getArrayDesc().getDimensions()[0].getLength();
    result.at(1) = array->getArrayDesc().getDimensions()[1].getLength();
    return result;
}


inline ScaLAPACKPhysical::matSize_t ScaLAPACKPhysical::getMatChunkSize(boost::shared_ptr<Array>& array) const
{
    assert(array->getArrayDesc().getDimensions().size() == 2);

    matSize_t result;
    result.at(0) = array->getArrayDesc().getDimensions()[0].getChunkInterval();
    result.at(1) = array->getArrayDesc().getDimensions()[1].getChunkInterval();
    return result;
}

} // namespace

#endif /* SCALAPACKPHYSICAL_HPP_ */
