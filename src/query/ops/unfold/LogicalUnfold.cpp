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

/*
 * LogicalUnfold.cpp
 *
 *  Created on: 13 May 2014
 *      Author: Dave Gosselin
 */

#include <query/Operator.h>

namespace scidb
{

  /**
   * @brief The operator: unfold().
   *
   * @par Synopsis:
   *   unfold( array )
   *
   * @par Summary:
   *   Complicated input data are often loaded into table-like 1-d
   *   multi-attribute arrays. Sometimes we want to assemble uniformly-typed
   *   subsets of the array attributes into a matrix, for example to compute
   *   correlations or regressions. unfold will transform the input
   *   array into a 2-d matrix whose columns correspond to the input array
   *   attributes. The output matrix row dimension will have a chunk size
   *   equal to the input array, and column chunk size equal to the number
   *   of columns. 
   *
   * @par Input:
   *   - array: the array to consume
   *
   * @par Output array:
   *        <
   *   <br> >
   *   <br> [
   *   <br> ]
   *
   * @par Examples:
   *   unfold(apply(build(<v:double>[i=0:9,3,0],i),w,i+0.5))
   *
   * @par Errors:
   *   SCIDB_LE_ILLEGAL_OPERATION
   *
   * @par Notes:
   *   n/a
   *
   */
  class LogicalUnfold : public LogicalOperator
  {
  public:
    LogicalUnfold(const string& logicalName,
		  const string& alias)
      : LogicalOperator(logicalName, alias) {
      ADD_PARAM_INPUT()
	_usage = "unfold(A)\n"
	"where:\n"
	"A is a n-d matrix with one or more uniformly-typed attributes.\n\n"
	"unfold(A) returns a n+1-d array that copies the attributes of A into\n"
	"the n+1st dimension of an output matrix.\n\n"
	"Note: The output matrix row dimension will have a chunk size equal\n"
	"to the input array, and column chunk size equal to the number of columns.\n\n"
	"EXAMPLE:\n\n"
	"unfold(apply(build(<v:double>[i=0:9,3,0],i),w,i+0.5))";
    }

    /**
     * Walk the attributes to see if any of them do not match the type
     * provided in the input schema.  In the event of a mismatch,
     * throw an exception.
     */
    void checkInputAttributes(const Attributes& attrs,
			      size_t nAttrs) throw(scidb::SystemException) {
      for (AttributeID i = 1; i < nAttrs; ++i) {
	if (attrs[i].getType() != attrs[0].getType()) {
	  throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
	    << "unfold requires that all input array attributes have the same type";
	}
      }
    }

    ArrayDesc inferSchema(vector<ArrayDesc> schemas,
			  shared_ptr<Query> query) {
      // Look at the first schema for the input schema; since the operator
      // takes only one input array, there should only be one schema.
      assert(schemas.size() == 1);
      const ArrayDesc& inputSchema = schemas[0];

      // Check the attributes to ensure that they are of the same type.
      const Attributes& attrs = inputSchema.getAttributes(true);
      size_t nAttrs = attrs.size();
      checkInputAttributes(attrs, nAttrs);

      // Create the output attributes and output dimensions; use them to
      // construct a new ArrayDesc and return it.
      Attributes outputAttributes;
      outputAttributes.push_back(AttributeDesc(0,
					       attrs[0].getName(),
					       attrs[0].getType(),
					       AttributeDesc::IS_NULLABLE,
					       0));
      outputAttributes = addEmptyTagAttribute(outputAttributes);

      // Effort to create a new dimension name that doesn't conflict
      // with existing dimensions: use the operator name and 
      // the number of dimensions on the input as the output
      // dimension name.
      Dimensions outputDimensions = inputSchema.getDimensions();
      ostringstream dim_name;
      dim_name << "unfold_"
	       << outputDimensions.size();
      outputDimensions.push_back(DimensionDesc(dim_name.str(),
					       0, 
					       nAttrs-1, 
					       nAttrs, 
					       0));
      return ArrayDesc(inputSchema.getName(), 
		       outputAttributes, 
		       outputDimensions);

    }
  };

  // This macro registers the operator with the system. The second argument is
  // the SciDB user-visible operator name that is used to invoke it.
  DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalUnfold, "unfold")

}  // namespace scidb
