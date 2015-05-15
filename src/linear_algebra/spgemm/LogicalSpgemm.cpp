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
 * LogicalSpgemm.cpp
 *
 *  Created on: November 4, 2013
 */

#include <query/Operator.h>

namespace scidb
{

inline bool hasSingleAttribute(ArrayDesc const& desc)
{
    return desc.getAttributes(true).size() == 1;
}

/**
 * @brief The operator: spgemm().
 *
 * @par Synopsis:
 *   spgemm( leftArray, rightArray [,semiring] )
 *
 * @par Summary:
 *   Produces a result array via matrix multiplication.
 *   Both matrices must have a single numeric attribute.
 *   The two matrices must have the same size of 'inner' dimension and same chunk size along that dimension.
 *
 * @par Input:
 *   - leftArray: the left matrix with two dimensions: leftDim1, leftDim2
 *   - rightArray: the right matrix with two dimensions: rightDim1, rightDim2
 *   - [semiring]: optional name of a semiring to be used instead of ordinary arithmetic (+,*)
 *                 when performing the matrix multiplication. Supported values are:
 *                 "min.+" -- the Tropical Semiring, i.e. a+b -> min(a,b) ; a*b -> a+b ;
 *                            the implicit sparse value is +inf.
 *                 "max.+" -- the Arctic Semiring,   i.e. a+b -> max(a,b) ; a*b -> a+b ;
 *                            the implicit sparse vlaue is -inf.
 *                 This option is useful for writing graph theoretic operations expressed
 *                 and computed as linear algebra.  An introduction to the subject suitable
 *                 for a computer scientist is: Stephan Dolan, "Fun with Semirings,
 *                 A functional perl on the abuse of linear algebra"
 *                 [http://www.cl.cam.ac.uk/~sd601/papers/semirings.pdf]
 *
 * @par Output array:
 *        <
 *   <br>   'multiply': the result attribute name
 *   <br> >
 *   <br> [
 *   <br>   leftDim1
 *   <br>   rightDim2
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalSpgemm : public  LogicalOperator
{
public:
    LogicalSpgemm(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES(); // a string that contains the named semiring option
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> >
        nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas) {

        // required by ADD_PARAM_VARIES() -- this is copy pasted, not understood
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        switch (_parameters.size()) {
        // either one or two extra parameters, both of which must be string
        case 0:
        case 1:
            res.push_back(PARAM_CONSTANT("string"));
            break;
        default:
            break;
        }

        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);

        if (!hasSingleAttribute(schemas[0]) || !hasSingleAttribute(schemas[1]))
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR2);
        if (schemas[0].getDimensions().size() != 2 || schemas[1].getDimensions().size() != 2)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR3);

        if (schemas[0].getDimensions()[0].getLength() == INFINITE_LENGTH
                || schemas[0].getDimensions()[1].getLength() == INFINITE_LENGTH
                || schemas[1].getDimensions()[0].getLength() == INFINITE_LENGTH
                || schemas[1].getDimensions()[1].getLength() == INFINITE_LENGTH)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR4);

        if (schemas[0].getDimensions()[1].getLength() != schemas[1].getDimensions()[0].getLength()
                || schemas[0].getDimensions()[1].getStartMin() != schemas[1].getDimensions()[0].getStartMin())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR5);

        if (schemas[0].getDimensions()[1].getChunkInterval() != schemas[1].getDimensions()[0].getChunkInterval())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR6);
        if (schemas[0].getAttributes()[0].getType() != schemas[1].getAttributes()[0].getType())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR7);
        if (schemas[0].getAttributes()[0].isNullable() || schemas[1].getAttributes()[0].isNullable())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR8);

        // only support built-in numeric types
        TypeId type = schemas[0].getAttributes()[0].getType();
        if (type!=TID_FLOAT &&
            type!=TID_DOUBLE
            ) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR9);
        }

        //
        // get the optional 3rd argument: the semiring string: "min.+" or "max.+". They only apply to float and double
        //  or the optional 3rd/4th argument: the rightReplicate flag
        //
        string namedOptionStr;
        switch (_parameters.size()) { // number of options remaining to be processed?
        case 0:
            break; // done
        case 1:
        case 2:
            typedef boost::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;
            namedOptionStr = evaluate(reinterpret_cast<ParamType_t&>(_parameters[0])->getExpression(), query, TID_STRING).getString();
            if (namedOptionStr != "min.+" &&
                namedOptionStr != "max.+" &&
                namedOptionStr != "rightReplicate=true" &&
                namedOptionStr != "rightReplicate=false" &&
                namedOptionStr != "count-mults") {
                throw(SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                      << "LogicalSpgemm::inferSchema(): unrecognized option '" << namedOptionStr << "'");
            }
            if (type != TID_FLOAT && type != TID_DOUBLE) {
                throw(SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                      << "LogicalSpgemm::inferSchema(): the 'min.+' and 'max.+' options support only float and double attribute types");
            }
            break;
        default:
            assert(false) ; // NOTREACHED
            // scidb::SCIDB_SE_SYNTAX::SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3 is thrown before this
            // line reached, this assert is only to ensure that it stays that way
            break;
        }


        Attributes atts(1, AttributeDesc((AttributeID)0, "multiply", type, 0, 0));

        Dimensions dims(2);
        DimensionDesc const& d1 = schemas[0].getDimensions()[0];
        dims[0] = DimensionDesc(d1.getBaseName(),
                                d1.getNamesAndAliases(), 
                                d1.getStartMin(), 
                                d1.getCurrStart(), 
                                d1.getCurrEnd(), 
                                d1.getEndMax(), 
                                d1.getChunkInterval(), 
                                0);

        DimensionDesc const& d2 = schemas[1].getDimensions()[1];
        dims[1] = DimensionDesc(d1.getBaseName() == d2.getBaseName() ? d1.getBaseName() + "2" : d2.getBaseName(),
                                d2.getNamesAndAliases(), 
                                d2.getStartMin(), 
                                d2.getCurrStart(), 
                                d2.getCurrEnd(), 
                                d2.getEndMax(), 
                                d2.getChunkInterval(), 
                                0);

        return ArrayDesc("Multiply", addEmptyTagAttribute(atts),dims);
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSpgemm, "spgemm");

} // end namespace scidb
