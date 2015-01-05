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
 * @file LogicalSave.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Save operator for saveing data from external files into array
 */

#include "query/Operator.h"
#include "smgr/io/ArrayWriter.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: save().
 *
 * @par Synopsis:
 *   save( srcArray, file, instanceId = -2, format = 'store' )
 *
 * @par Summary:
 *   Saves the data in an array to a file.
 *
 * @par Input:
 *   - srcArray: the source array to save from.
 *   - file: the file to save to.
 *   - instanceId: positive number means an instance ID on which file will be saved.
 *                 -1 means to save file on every instance. -2 - on coordinator.
 *   - format: @c ArrayWriter format in which file will be stored
 *
 * @see ArrayWriter::isSupportedFormat
 *
 * @par Output array:
 *   the srcArray is returned
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
/**
 * Must be called as SAVE('existing_array_name', '/path/to/file/on/instance')
 */
class LogicalSave: public LogicalOperator
{
public:
    LogicalSave(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
		ADD_PARAM_CONSTANT("string")//0
		ADD_PARAM_VARIES();          //2
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        switch (_parameters.size()) {
          case 0:
            assert(false);
            break;
          case 1:
            res.push_back(PARAM_CONSTANT("int64"));
            break;
          case 2:
            res.push_back(PARAM_CONSTANT("string"));
            break;
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        assert(_parameters.size() >= 1);
        
        if (_parameters.size() >= 3) { 
            Value v = evaluate(
                ((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                query, TID_STRING);
            string const& format = v.getString();

            if (!format.empty()
                && compareStringsIgnoreCase(format, "auto") != 0
                && !ArrayWriter::isSupportedFormat(format))
            {
                throw  USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                            SCIDB_LE_UNSUPPORTED_FORMAT,
                                            _parameters[2]->getParsingContext())
                    << format;
            }
        }

        return inputSchemas[0];
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSave, "save")


} //namespace
