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


// de-facto standards
#include <boost/numeric/conversion/cast.hpp>

// scidb imports
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/BlockCyclic.h>

// SciDB
#include "../../scalapackUtil/scalapackFromCpp.hpp"
#include "../../DLAErrors.h"

namespace scidb
{

// REFACTOR
inline bool hasSingleAttribute(ArrayDesc const& desc)
{
    return desc.getAttributes().size() == 1 ||
           (desc.getAttributes().size() == 2 && desc.getAttributes()[1].isEmptyIndicator());
}

// REFACTOR
template<typename int_tt>
inline int_tt divCeil(int_tt val, int_tt divisor) {
    return (val + divisor - 1) / divisor ;
}

class MPICopyLogical: public LogicalOperator
{
public:
    MPICopyLogical(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query);
};

ArrayDesc MPICopyLogical::inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
{
    assert(schemas.size() == 1);

    if (!hasSingleAttribute(schemas[0]))
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR2);
    if (schemas[0].getDimensions().size() != 2)
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR3);
    if (schemas[0].getAttributes()[0].getType() != TID_DOUBLE)
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR5);

    const Dimensions dims = schemas[0].getDimensions();
    DimensionDesc const& d0 = dims[0]; // kill these two off, to match most code?
    DimensionDesc const& d1 = dims[1];

    // must have finite size
    if (d0.getLength() == INFINITE_LENGTH
        || d1.getLength() == INFINITE_LENGTH)
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR9);

    // is there a ctor that will take just the original names and do the following by a rule?
    Attributes atts(1);
    atts[0] = AttributeDesc((AttributeID)0, "copy", TID_DOUBLE, 0, 0);
    Dimensions outDims(2);
    outDims[0] = DimensionDesc(d0.getBaseName() + "_1", 
                            d0.getStartMin(),
                            d0.getCurrStart(),
                            d0.getCurrEnd(),
                            d0.getEndMax(), 
                            d0.getChunkInterval(), 
                            0);
    outDims[1] = DimensionDesc(d0.getBaseName() + "_2", 
                            d1.getStartMin(),
                            d1.getCurrStart(),
                            d1.getCurrEnd(),
                            d1.getEndMax(), 
                            d1.getChunkInterval(), 
                            0);
    return ArrayDesc("mpicopy", atts, outDims);
}

REGISTER_LOGICAL_OPERATOR_FACTORY(MPICopyLogical, "mpicopy");

} // end namespace

