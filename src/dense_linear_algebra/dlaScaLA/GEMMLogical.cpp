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

// C++
// std C
#include <stdlib.h>
// de-facto standards
#include <boost/numeric/conversion/cast.hpp>
// SciDB
#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/BlockCyclic.h>

// MPI/ScaLAPACK
#include <scalapackUtil/dimUtil.hpp>
#include <scalapackUtil/ScaLAPACKLogical.hpp>
// local
#include "GEMMOptions.hpp"
#include "DLAErrors.h"


using namespace scidb;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra.ops.gemm"));

namespace scidb
{
 ///
 /// @brief The operator: gemm().
 ///
 /// @par Synopsis:
 ///   gemm( leftArray, rightArray, accumulateArray )
 ///
 /// @par Summary:
 ///   Produces a result array via matrix multiplication of leftArray with rightArray and addition of accumulateArray
 ///   All matrices must have a single numeric attribute of type 'double', two dimensions, and the chunk size of 32x32
 ///   leftArray and rightArray must have the same size of 'inner' dimension, i.e. leftArray second dimension and rightArray first dimension.
 ///    acumulateArray must have the shape of a matrix-multiplication-product, i.e. leftArray first dimension by rightArray second dimension.
 ///
 /// @par Input:
 ///   - leftArray: the left matrix with two dimensions: leftDim1, leftDim2
 ///   - rightArray: the right matrix with two dimensions: rightDim1, rightDim2
 ///
 /// @par Output array:
 ///        <
 ///   <br>   <double:gemm>: the result attribute
 ///   <br> >
 ///   <br> [
 ///   <br>   leftDim1
 ///   <br>   rightDim2
 ///   <br> ]
 ///
 /// @par Examples:
 ///   n/a
 ///
 /// @par Errors:
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR2 -- if attribute count != 1
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR5 -- if attribute type is not double in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR3 -- if number of dimensions != 2 in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR9 -- if sizes are not bounded in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR41 -- if chunk interval is too small in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR42 -- if chunk interval is too large in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR40 -- if there is chunk overlap in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR10 -- if the chunk sizes in any of the input arrays are not identical (until auto-repart is working)
 ///
 /// @par Notes:
 ///   n/a
 ///
class GEMMLogical: public LogicalOperator
{
public:
    GEMMLogical(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES(); // a string that contains the named TRANS[A|B], ALPHA, and/or BETA options
        // TODO: Note that TRANS is the standard ScaLAPACK shorthand for transpose or conjugate transpose
        // TODO: Once checked in, give S.Marcus the pointer to the TRANS,ALPHA,BETA doc from ScaLAPACK
        //       and have him include the pointer to the netlib page "for more detail"
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query);

    typedef std::vector<boost::shared_ptr<OperatorParamPlaceholder> > ParamPlaceholders_t ;
    ParamPlaceholders_t nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas);
};

// required by ADD_PARAM_VARIES()
GEMMLogical::ParamPlaceholders_t GEMMLogical::nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
{
    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
    res.push_back(END_OF_VARIES_PARAMS());
    switch (_parameters.size()) {
    case 0:
        res.push_back(PARAM_CONSTANT("string"));
        break;
    default:
        break;
    }

    return res;
}

ArrayDesc GEMMLogical::inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
{
    LOG4CXX_TRACE(logger, "GEMMLogical::inferSchema(): begin.");

    enum dummy  {ROW=0, COL=1};
    enum dummy2 {AA=0, BB, CC, NUM_MATRICES};  // which matrix: f(AA,BB,CC) = alpha AA BB + beta CC

    //
    // array checks (first 3 arguments)
    //
    assert(schemas.size() == NUM_MATRICES);
    checkScaLAPACKInputs(schemas, query, NUM_MATRICES, NUM_MATRICES);

    //
    // get the optional 4th argument: the parameters string: ( TRANSA, TRANSB, ALPHA, BETA)
    //
    string namedOptionStr;
    switch (_parameters.size()) {
    case 0:
        break;
    case 1:
        typedef boost::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;
        namedOptionStr = evaluate(reinterpret_cast<ParamType_t&>(_parameters[0])->getExpression(), query, TID_STRING).getString();
        break;
    default:
        assert(false) ; // NOTREACHED
        // scidb::SCIDB_SE_SYNTAX::SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3 is thrown before this
        // line reached, this assert is only to ensure that it stays that way
        break;
    }
    GEMMOptions options(namedOptionStr);  // convert option string to the 4 values

    //
    // cross-matrix constraints:
    //

    // check: cross-argument sizes
    if (nCol(schemas[AA], options.transposeA) != nRow(schemas[BB], options.transposeB)) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "first matrix columns must equal second matrix rows (after optional transposes) ");
    }
    if (nRow(schemas[AA], options.transposeA) != nRow(schemas[CC])) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "first and third matrix must have equal number of rows (after optional 1st matrix transpose)");
    }
    if (nCol(schemas[BB], options.transposeB) != nCol(schemas[CC])) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "first and third matrix must have equal number of columns (after optional 1st matrix transpose)");
    }

    // TODO: check: ROWS * COLS is not larger than largest ScaLAPACK fortran INTEGER

    // TODO: check: total size of "work" to scalapack is not larger than largest fortran INTEGER
    //       hint: have Cmake adjust the type of slpp::int_t
    //       hint: maximum ScaLAPACK WORK array is usually determined by the function and its argument sizes


    //
    // inputs look good, create and return the output schema
    // note that the output has the dimensions and name bases of the third argument C
    // so that we can iterate on C, by repeating the exact same query,
    // NOTE: we are SUPER careful not to change its dim names if they are already distinct.
    //       to make the iteration as simple as possible
    //
    const Dimensions& dimsCC = schemas[CC].getDimensions();
    
    std::pair<string, string> distinctNames = ScaLAPACKDistinctDimensionNames(dimsCC[ROW].getBaseName(),
                                                                              dimsCC[COL].getBaseName());
    Dimensions outDims(2);
    outDims[ROW] = DimensionDesc(distinctNames.first,
                                 dimsCC[ROW].getStartMin(),
                                 dimsCC[ROW].getCurrStart(),
                                 dimsCC[ROW].getCurrEnd(),
                                 dimsCC[ROW].getEndMax(),
                                 dimsCC[ROW].getChunkInterval(),
                                 0);

    outDims[COL] = DimensionDesc(distinctNames.second,
                                 dimsCC[COL].getStartMin(),
                                 dimsCC[COL].getCurrStart(),
                                 dimsCC[COL].getCurrEnd(),
                                 dimsCC[COL].getEndMax(),
                                 dimsCC[COL].getChunkInterval(),
                                 0);

    Attributes atts(1); atts[0] = AttributeDesc(AttributeID(0), "gemm", TID_DOUBLE, 0, 0);

    LOG4CXX_TRACE(logger, "GEMMLogical::inferSchema(): end.");
    return ArrayDesc("GEMM", addEmptyTagAttribute(atts), outDims);
}

REGISTER_LOGICAL_OPERATOR_FACTORY(GEMMLogical, "gemm");

} //namespace
