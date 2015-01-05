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
 * @file UniqSettings.h
 * A common settings structure for the uniq operator. This class uses the same settings pattern as introduced in
 * InstanceStatsSettings.h
 * @author apoliakov@paradigm4.com
 */

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>

#ifndef UNIQ_SETTINGS
#define UNIQ_SETTINGS

#define DEFAULT_CHUNK_SIZE 1000000

namespace scidb
{

/**
 * Very simple; has only one optional output parameter - the output chunk size.
 */
class UniqSettings
{
private:
    size_t _outputChunkSize;

public:
    static const size_t MAX_PARAMETERS = 1;

    /**
     * Vanilla
     * @param operatorParameters as passed to the operator
     * @param logical true if clled with Logical parameters, else physical
     * @param query the query context
     */
    UniqSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                 bool logical,
                 shared_ptr<Query>& query):
       _outputChunkSize(DEFAULT_CHUNK_SIZE)
    {
        string const chunkSizeParamHeader = "chunk_size=";
        size_t nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                  << "illegal number of parameters passed to UniqSettings";
        }
        for (size_t i= 0; i<nParams; ++i)
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[i];
            string parameterString;
            if (logical)
            {
                parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(),query, TID_STRING).getString();
            }
            else
            {
                parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
            }
            if (starts_with(parameterString, chunkSizeParamHeader))
            {
                string paramContent = parameterString.substr(chunkSizeParamHeader.size());
                trim(paramContent);
                int64_t sval;
                try
                {
                    sval = lexical_cast<int64_t> (paramContent);
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANNOT_PARSE_INTEGER_PARAMETER) << parameterString;
                }
                if (sval <= 0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_PARAMETER_NOT_POSITIVE_INTEGER) << parameterString;
                }
                _outputChunkSize = sval;
            }
            else
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNRECOGNIZED_PARAMETER) << parameterString;
            }
        }
    }

    /**
     * @return the chun size (default or user-set as the case may be)
     */
    size_t outputChunkSize() const
    {
        return _outputChunkSize;
    }
};

}

#endif //UNIQ_SETTINGS
