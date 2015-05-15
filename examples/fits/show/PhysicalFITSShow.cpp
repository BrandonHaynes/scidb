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

/**
 * @author Miguel Branco <miguel@spacebase.org>
 *
 * @see LogicalFITSShow.cpp
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/TupleArray.h"
#include "system/SystemCatalog.h"

#include "../common/FITSParser.h"


namespace scidb
{
using namespace std;


class PhysicalFITSShow: public PhysicalOperator
{
public:
    PhysicalFITSShow(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
       return ArrayDistribution(psLocalInstance);
    }

    boost::shared_ptr<Array> execute(vector<boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        uint32_t hdu = 0;

        if (! query->isCoordinator() )
        {
            return shared_ptr<Array> (new MemArray(_schema,query));
        }

        shared_ptr<TupleArray> tuples(make_shared<TupleArray>(_schema, _arena));
        const string filePath = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();

        FITSParser parser(filePath);

        while (true) {
            try {
                Value tuple[3];
                string error;
                if (parser.moveToHDU(hdu, error)) {
                    tuple[0].setBool(true);

                    switch (parser.getBitPixType()) {
                    case FITSParser::INT16:
                        tuple[1].setString("int16");
                        break;
                    case FITSParser::INT16_SCALED:
                        tuple[1].setString("float");
                        break;
                    case FITSParser::INT32:
                        tuple[1].setString("int32");
                        break;
                    case FITSParser::INT32_SCALED:
                        tuple[1].setString("float");
                        break;
                    case FITSParser::FLOAT32_SCALED:
                        tuple[1].setString("float");
                        break;
                    default:
                        SCIDB_UNREACHABLE();
                    }

                    stringstream ss;
                    ss << "BITPIX=" << parser.getBitPix();

                    vector<int> const& axisSizes = parser.getAxisSizes();
                    ss << ",NAXIS=" << axisSizes.size();
                    for (size_t i = 0; i < axisSizes.size(); i++) {
                        ss << "," << "NAXIS" << (i + 1) << "=" << axisSizes[i];
                    }

                    tuple[2].setString(ss.str().c_str());
                } else {
                    tuple[0].setBool(false);
                    tuple[1].setNull();
                    tuple[2].setNull();
                }
                tuples->appendTuple(tuple);
            } catch (Exception&) {
                break;
            }
            ++hdu;
        }

        return tuples;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFITSShow, "fits_show", "impl_fits_show");

}
