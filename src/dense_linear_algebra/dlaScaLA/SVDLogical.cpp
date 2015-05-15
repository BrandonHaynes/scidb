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

#include <boost/numeric/conversion/cast.hpp>

#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/BlockCyclic.h>

#include "DLAErrors.h"
#include "scalapackUtil/ScaLAPACKLogical.hpp"

namespace scidb
{

inline bool hasSingleAttribute(ArrayDesc const& desc)
{
    return desc.getAttributes().size() == 1 || (desc.getAttributes().size() == 2 && desc.getAttributes()[1].isEmptyIndicator());
}

///
/// handy inline, rounds up instead of down like int division does
/// good for, e.g. calculating block sizes
template<typename int_tt>
inline int_tt divCeil(int_tt val, int_tt divisor) {
    return (val + divisor - 1) / divisor ;
}
 ///
 /// @brief The operator: gesvd().
 ///
 /// @par Synopsis:
 ///   gesvd( inputArray, factor )
 ///
 /// @par Summary:
 ///   Produces a singular value decomposition (SVD) of the inputArray matrix and returns one of the three decomposition factors.
 ///   The input matrix must have a single numeric attribute of type 'double', two dimensions, and the chunk size of 32x32
 ///
 /// @par Input:
 /// <br>  - inputArray: an array with two dimensions (i.e. matrix): dim1, dim2
 /// <br>  - factor: a string identifying the factor of SVD, either
 /// <br>        'U' (aka 'left')
 /// <br>        or
 /// <br>        'VT' (aka 'right')
 /// <br>        or
 /// <br>        'S' (aka 'SIGMA','values') 
 ///
 /// @par Output array:
 ///   <br> <
 ///   <br>   <double:u> or <double:v> or <double:sigma>: the result attribute corresponding to the SVD factor
 ///   <br> >
 ///   <br> For U:
 ///   <br> [
 ///   <br>   dim1
 ///   <br>   dim1
 ///   <br> ]
 ///   <br> For VT:
 ///   <br> [
 ///   <br>   dim2
 ///   <br>   dim2
 ///   <br> ]
 ///   <br> For S:
 ///   <br> [
 ///   <br>   dim2
 ///   <br> ]
 ///
 /// @par Examples:
 ///   gesvd( inputArray, 'U' )
 ///   gesvd( inputArray, 'VT' )
 ///   gesvd( inputArray, 'S' )
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
class SVDLogical: public LogicalOperator
{
public:
    SVDLogical(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_CONSTANT("string")
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query);
};

ArrayDesc SVDLogical::inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
{
    enum dumm { SINGLE_MATRIX = 1 };
    assert(schemas.size() == SINGLE_MATRIX);

    if(schemas.size() < 1)
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR2);

    //
    // per-array checks
    //
    checkScaLAPACKInputs(schemas, query, SINGLE_MATRIX, SINGLE_MATRIX);

    // TODO: check: ROWS * COLS is not larger than largest ScaLAPACK fortran INTEGER

    // TODO: check: total size of "work" to scalapack is not larger than largest fortran INTEGER
    //       hint: have Cmake adjust the type of slpp::int_t
    //       hint: maximum ScaLAPACK WORK array is usually determined by the function and its argument sizes

    const string& whichMatrix = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(), query, TID_STRING).getString();

    const Dimensions& dims = schemas[0].getDimensions();
    size_t minRowCol = std::min(dims[0].getLength(),
                                dims[1].getLength());

    const size_t ZERO_OUTPUT_OVERLAP = 0;
    // TODO: Question: Should these be case-insensitive matches?
    if (whichMatrix == "U" || whichMatrix == "left") // most frequent, and less-frequent names
    {
        Dimensions outDims(2);
        std::pair<string, string> distinctNames = ScaLAPACKDistinctDimensionNames(dims[0].getBaseName(),
                                                                                  "i"); // conventional subscript for sigma
        // nRow out is in the same space as nRow in
        outDims[0] = DimensionDesc(distinctNames.first,
                                   dims[0].getStartMin(),
                                   dims[0].getCurrStart(),
                                   dims[0].getCurrEnd(),
                                   dims[0].getEndMax(),
                                   dims[0].getChunkInterval(),
                                   ZERO_OUTPUT_OVERLAP);

       // nCol out has size min(nRow,nCol).  It takes us to the subspace of the diagonal matrix "SIGMA"
       // note that it in a different basis than the original, so it cannot actually
       // share any meaningful integer or non-integer array dimensions with them.
       // therefore it uses just the interval 0 to minRowCol-1
       outDims[1] = DimensionDesc(distinctNames.second,
                                  Coordinate(0),                // start
                                  Coordinate(0),                // curStart
                                  Coordinate(minRowCol - 1),    // end
                                  Coordinate(minRowCol - 1),    // curEnd
                                  dims[1].getChunkInterval(),   // inherit
                                  ZERO_OUTPUT_OVERLAP);

        Attributes atts(1); atts[0] = AttributeDesc((AttributeID)0, "u", TID_DOUBLE, 0, 0);
        ArrayDesc result("U", addEmptyTagAttribute(atts), outDims);
        log4cxx_debug_dimensions("SVDLogical::inferSchema(U)", result.getDimensions());
        return result;
    }
    else if (whichMatrix == "VT" || whichMatrix == "right")
    {
        Dimensions outDims(2);
        std::pair<string, string> distinctNames = ScaLAPACKDistinctDimensionNames("i", // conventional subscript for sigma
                                                                                  dims[1].getBaseName());

        // nRow out has size min(nRow,nCol). It takes from the subspace of the diagonal matrix "SIGMA"
        // note that it in a different basis than the original, so it cannot actually
        // share any meaningful integer or non-integer array dimensions with them.
        // therefore it uses just the interval 0 to minRowCol-1
        outDims[0] = DimensionDesc(distinctNames.first,
                                   Coordinate(0),  // start
                                   Coordinate(0),  // curStart
                                   Coordinate(minRowCol - 1), // end
                                   Coordinate(minRowCol - 1), // curEnd
                                   dims[0].getChunkInterval(), // inherit
                                   ZERO_OUTPUT_OVERLAP);

        // nCol out is in the same space as nCol in
        outDims[1] = DimensionDesc(distinctNames.second,
                                dims[1].getStartMin(),
                                dims[1].getCurrStart(),
                                dims[1].getCurrEnd(),
                                dims[1].getEndMax(),
                                dims[1].getChunkInterval(),
                                ZERO_OUTPUT_OVERLAP);

        Attributes atts(1); atts[0] = AttributeDesc((AttributeID)0, "v", TID_DOUBLE, 0, 0);
        ArrayDesc result("VT", addEmptyTagAttribute(atts), outDims);
        log4cxx_debug_dimensions("SVDLogical::inferSchema(VT)", result.getDimensions());
        return result;
    }
    else if (whichMatrix == "S" || whichMatrix == "SIGMA" || whichMatrix == "values")
    {
        Dimensions outDims(1);
        // nRow out has size min(nRow,nCol), and is not in the same dimensional space as the original
        // note that it in a different basis than the original, so it cannot actually
        // share any meaningful integer or non-integer array dimensions with them.
        // therefore it uses just the interval 0 to minRowCol-1
        outDims[0] = DimensionDesc("i",            // conventional subscript for sigma
                                   Coordinate(0),  // start
                                   Coordinate(0),  // curStart
                                   Coordinate(minRowCol - 1), // end
                                   Coordinate(minRowCol - 1), // curEnd
                                   dims[0].getChunkInterval(), // inherit
                                   ZERO_OUTPUT_OVERLAP);

        Attributes atts(1); atts[0] = AttributeDesc((AttributeID)0, "sigma", TID_DOUBLE, 0, 0);
        ArrayDesc result("SIGMA", addEmptyTagAttribute(atts), outDims);
        log4cxx_debug_dimensions("SVDLogical::inferSchema(SIGMA)", result.getDimensions());
        return result;
    } else {
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR33);
    }
}

REGISTER_LOGICAL_OPERATOR_FACTORY(SVDLogical, "gesvd");

} //namespace
