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
/// PhysicalSplitArrayTest.cpp
///

#include <array/Metadata.h>
#include <array/DelegateArray.h>
#include <log4cxx/logger.h>
#include <query/Operator.h>


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.test.splitarray.LogicalSplitArrayTest"));

class PhysicalSplitArrayTest: public PhysicalOperator
{
public:
    PhysicalSplitArrayTest(const std::string& logicalName, const std::string& physicalName,
                           const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        _schema = schema;
    }

    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
        shared_ptr<Array> input = inputArrays[0];

        Dimensions const& dims = _schema.getDimensions();
        size_t nRow = dims[0].getLength();
        size_t nCol = dims[1].getLength();

        // cS = chunk size
        size_t cSCol = dims[1].getChunkInterval();

        size_t instanceID = query->getInstanceID();
        const size_t nInstances = query->getInstancesCount();
        if (nInstances > 1) {
            input = redistributeToRandomAccess(input, query, psByCol,
                                               ALL_INSTANCE_MASK,
                                               shared_ptr<DistributionMapper>(),
                                               0,
                                               shared_ptr<PartitioningSchemaData>());
        }

        // the following is somewhat convoluted, and can be cleaned up, if deemed necessary, as a later step
        // (after first check in and test against SplitArray).
        // The reason it should be committed this way (at least at first) is that it is a donated test case based on
        // transliteration of code from an external client library which already uses SplitArray.
        // We want to guarantee that this test case works for the client library whether convoluted or not,
        // in preference to cleaning it up and having it accidentally be less representative of what the client
        // depends on.
        // After we have checked in a SplitArray that passes this test, we could consider changing the code,
        // but only if care is taken to make sure that the revised code is still exactly reprentative of what
        // the client code depends on.
        //
        // I have attempted to improve the situation by giving variables better names (strictly by search-and-replace),
        // using a careful substitution of the factored "roundUpToMultiple," for a repeated pattern,
        // and adding comments.

        // bS = blockSize, a multiple of chunkSize
        size_t bSCol = roundUpToMultiple(nCol, cSCol * nInstances) * cSCol ;
        if (!nRow || !nCol || !bSCol)
        return shared_ptr<Array>(new MemArray(_schema,query));

        // check for participation (may have more instances than array can use)
        size_t usedInstances = roundUpToMultiple(nCol, bSCol);
        if (usedInstances < nInstances) {
            if (query->getInstanceID() >= usedInstances) {
                return shared_ptr<Array>(new MemArray(_schema,query));
            }
        }

        // calculate size of the local share of the data
        size_t nColLocalFull = roundUpToMultiple(nCol, usedInstances); // nCol of this instance's data, if full column group
        size_t colStartOffset = nColLocalFull * instanceID ;           // nCol to the left of our local data
        size_t nColLocal = std::min(nColLocalFull, std::max(size_t(0), nCol - colStartOffset)); // last instance will have fewer columns
                                                                       // except when the global width is an exact multiple of nColLocalFull

        // generate test data in memory
        // write the row-major ordering of the cells into matrix, this is the test result
        // (this is useful because in the past we've had trouble with partial chunks at the end having their data
        //  show up in the wrong place, or give garbage values instead)  In this test case, each cell must take
        // on a known value that can be easily reproduced using build() and then compared with this operator
        double* matrix = new double[nRow * nColLocal]; // need nColLocalFull option, can live with nColLocal at present
                                                       // (need to review 2 client codes to be sure)
                                                           // Q: why nColLocalFull option?
                                                           // A: some libraries based on standard
                                                           // numerical codes round allocations up
                                                           // to a memory blocksize.


        for (size_t matRow = 0; matRow < nRow; matRow++) {
            for (size_t matColLocal = 0; matColLocal < nColLocal; matColLocal ++) {
                //Tests should output the same data, regardless of the number of instances
                size_t rowMajorIndexLocal = matRow * nColLocal + matColLocal;
                size_t val = matRow * nCol + colStartOffset + matColLocal;
                matrix[rowMajorIndexLocal] = val;
            }
        }

        // create split array from _schema, matrix, and _schema coordinates
        shared_array<char> matrixShared(reinterpret_cast<char*>(matrix));
        Coordinates first(2), last(2);
        first[0] = dims[0].getStartMin();
        first[1] = dims[1].getStartMin() + colStartOffset;
        last[0] = dims[0].getEndMax();
        last[1] = first[1] + nColLocal - 1;

        return boost::shared_ptr<Array>(new SplitArray(_schema, matrixShared, first, last, query));
    }

private:
    size_t      roundUpToMultiple(size_t val, size_t multiplier) { return (val + (multiplier-1)) / multiplier; }
    size_t      localSize(size_t nRowCol, size_t usedInstances) { return roundUpToMultiple(nRowCol, usedInstances); }
    ArrayDesc   _schema;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSplitArrayTest, "splitarraytest", "PhysicalSplitArrayTest")

} //namespace scidb
