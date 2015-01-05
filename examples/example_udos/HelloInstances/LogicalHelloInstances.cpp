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
 * @file LogicalHelloInstances.cpp
 * A sample UDO that outputs an array containing a "Hello, World" string from every instance.
 * This is the most basic UDO designed to provide a starting example and an introduction to some SciDB internals.
 * As-is, the operator is barely useful.
 *
 * @brief The operator: hello_instances().
 *
 * @par Synopsis:
 *   <br> hello_instances()
 *
 * @par Summary:
 *   <br>
 *   Produces a result array with one cell for each running SciDB instance, and a single string "hello world"
 *   attribute.
 *   <br>
 *   To add some usefulness, the operator may be extended to return some basic CPU, disk and RAM usage information
 *   from every instance, and then used for system monitoring purporses.
 *
 * @par Input: none
 *
 * @par Output array:
 *   <br> <
 *   <br>   instance_status:string
 *   <br> >
 *   <br> [
 *   <br>   instance_no = 0:*,1 0]
 *   <br> ]
 *
 * @author apoliakov@paradigm4.com
 */

#include <query/Operator.h>

namespace scidb //not required
{

/**
 * The Logical Operator object for hello_instances.
 * The primary objective of this class is to
 * <br> - check to make sure all the inputs are correct and
 * <br> - infer the shape (schema) of the output array, given these inputs
 *  Reminder: all operators accept zero or more arrays and parameters, and return a single array.
 */
class LogicalHelloInstances : public LogicalOperator
{
public:
    /**
     * All LogicalOperator constructors have the same signature and list the acceptable inputs here.
     * In this case, the operator does not accept any inputs.
     * @param logicalName used internally by scidb
     * @param alias used internally by scidb
     * @see LogicalInstanceStats.cpp for an example on how to list inputs.
     */
    LogicalHelloInstances(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {}

    /**
     * Determine the schema of the output. inferSchema is called on the coordinator instance during query planning and
     * may be called several times as the planner gets its act together. It will always be called with the same inputs
     * for the same query. This function must behave deterministically, but the shape of the output may vary based on
     * inputs and parameters.
     * @param schemas all of the schemas of the input arrays (if the operator accepts any)
     * @param query the query context
     * @return the schema of the outpt, as described above.
     */
    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        /*
         * Make one string attribute: id=0, name="instance_status" of type string, no flags, no default compression.
         * The ID of the attribute is simply a number from 0 to num_attributes-1 and must equal to its position
         * in the attributes vector.
         */
        AttributeDesc outputAttribute (0, "instance_status", TID_STRING, 0, 0);
        Attributes outputAttributes(1, outputAttribute);

        /* Add the empty tag attribute. Arrays with the empty tag are "emptyable" meaning that some cells may be empty.
         * It is a good practice to add this to every constructed array. In fact, in the future it may become the
         * default for all arrays.
         */
        outputAttributes = addEmptyTagAttribute(outputAttributes);

        /* The output dimension: from 0 to "*" with a chunk size of 1. The amount of data returned is so small that the
         * chunk size is not relevant.
         */
        DimensionDesc outputDimension("instance_no", 0, MAX_COORDINATE, 1, 0);
        Dimensions outputDimensions(1, outputDimension);

        /* The first argument is the name of the returned array. */
        return ArrayDesc("hello_instances", outputAttributes, outputDimensions);
    }
};

//This macro registers the operator with the system. The second argument is the SciDB user-visible operator name that is
//used to invoke it.
REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalHelloInstances, "hello_instances");

} //namespace scidb
