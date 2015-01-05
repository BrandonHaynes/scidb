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
 * @file PhysicalHelloInstances.cpp
 * The physical implementation of the hello_instances operator.
 * @see LogicalHelloInstances.cpp
 * @author apoliakov@paradigm4.com
 */

#include <query/Operator.h>

namespace scidb
{

/**
 * The primary responsibility of the PhysicalOperator is to return the proper array output as the result of the
 * execute() function.
 */
class PhysicalHelloInstances : public PhysicalOperator
{
public:
    /**
     * Looks the same for all operators. All the arguments are for SciDB internal use.
     * The operator is first constucted on the coordinator during planning (possibly several times), then constructed
     * on every instance to execute. Setting internal state as a result of construction is not reliable. To avoid
     * shared-pointer cycles and potential errors, it is recommended that operators do not have any additional member
     * variables.
     */
    PhysicalHelloInstances(string const& logicalName,
                           string const& physicalName,
                           Parameters const& parameters,
                           ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    /**
     * Execute the operator and return the output array. The input arrays (with actual data) are provided as an
     * argument. Non-array arguments to the operator are set in the _parameters member variable. This particular
     * operator has no arguments. The result of the Logical***::inferSchema() method is also provided as the member
     * variable _schema. Execute is called once on each instance.
     * @param inputArrays the input array arguments. In this simple case, there are none.
     * @param query the query context
     * @return the output array object
     */
    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        /* Find my instanceId from the query. Query has many useful methods like
         * - the total number of instances
         * - the id of the coordinator
         * - check if the query was cancelled.. and so on
         */
        InstanceID instanceId = query->getInstanceID();
        ostringstream outputString;
        outputString<<"Hello, World! This is instance "<<instanceId;

        /* Construct the output array. A MemArray is a general materialized array that can be read and written to.
         * Despite the name, the MemArray is actually backed by a LRU cache and chunks that are not currently open for
         * reading and writing are saved to disk, should the array size exceed the MEM_ARRAY_THRESHOLD setting. _schema
         * came from LogicalHelloInstances::inferSchema() and was shipped to all instanced by scidb.
         */
        shared_ptr<Array> outputArray(new MemArray(_schema, query));
        /* return outputArray; -- at this point this would return an empty array */

        /* In order to write data to outputArray, we create an ArrayIterator. The argument given is the attribute ID.
         * The ArrayIterator allows one to read existing chunks and add new chunks to the array.
         */
        shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);

        /* We are adding one chunk in the one-dimensional space. All chunks have a position, which is also the position
         * of the top-left element in the chunk. In this simple example, each chunk contains only once cell and this is
         * where the cell shall be written to.
         */
        Coordinates position(1, instanceId);

        /* Create the chunk and open a ChunkIterator to it. */
        shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, 0);

        /* Set the position inside the chunk */
        outputChunkIter->setPosition(position);

        /* The Value is a generic variable-size container for one attribute at one particular position. It also contains
         * a null-code (if the value is NULL) and information about the binary size of the data.
         */
        Value value;

        /* Copy the output string into the value.*/
        value.setString(outputString.str().c_str());

        /* Write the value into the chunk. */
        outputChunkIter->writeItem(value);

        /* Finish writing the chunk. After this call, outputChunkIter is invalidated. */
        outputChunkIter->flush();

        return outputArray;

        /* But what about the empty tag? Note that it is created implicitly, as a convenience, based on the flags we've
         * passed to the chunk.getIterator() call.
         * Interesting flags to chunk.getIterator include:
         * ChunkIterator::NO_EMPTY_CHECK - means do not create the empty tag implicitly. It then has to be written
         *    explicitly or via a different chunk. It is useful for writing multiple attributes.
         * ChunkIterator::SEQUENTIAL_WRITE - means the chunk shall be written in row-major order as opposed to
         *    random-access order. In this case, a faster write path is used. Row-major order means the last dimension
         *    is incremented first, up until the end of the chunk, after which the second-to last dimension is
         *    incremented by one and the last dimension starts back the beginning of the chunk - and so on.
         * ChunkIterator::APPEND_CHUNK - means append new data to the existing data already in the chunk; do not
         *    overwrite
         */

        /* Also note that this instance returns one chunk of the array. The entire array contains one chunk per
         * instance. If this is the root operator in the query, SciDB will automatically assemble all the chunks
         * from different instances to return to the front end. Otherwise, the next operator in the query will be
         * called on just the portion of the data returned on the local instance.
         *
         * Read operators uniq and index_lookup for advanced data distribution topics.
         */
    }
};

/* In this registration, the second argument must match the AFL operator name and the name provided in the Logical..
 * file. The third argument is arbitrary and used for debugging purposes.
 */
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalHelloInstances, "hello_instances", "PhysicalHelloInstances");

} //namespace scidb
