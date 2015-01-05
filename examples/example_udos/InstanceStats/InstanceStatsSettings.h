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
 * @file InstanceStatsSettings.h
 * A common settings structure for the instance_stats operator. This class illustrates a pattern of factoring out
 * parameters and parameter checks to an external class that is then used by both - the Physical and Logical operators.
 * Consider reading the LogicalInstanceStats code first before this class.
 * @see LogicalInstanceStats.cpp
 * @author apoliakov@paradigm4.com
 */

#include <boost/algorithm/string.hpp>
#include <query/Operator.h>

#ifndef INSTANCE_STATS_SETTINGS
#define INSTANCE_STATS_SETTINGS

namespace scidb
{

/*
 * An object constructed from the operator parameters that is then used to check the parameters' validity and tell the
 * operator code how to behave.
 */
class InstanceStatsSettings
{
private:
    /* The "Set" flags are used to warn the user not to set the same parameter multiple times */
    bool _dumpDataToLog;
    bool _dumpDataToLogSet;
    bool _global;
    bool _globalSet;

    /**
     * Very simple and somewhat rude parser used for all boolean flags.
     */
    void parseBooleanParameter(string const& paramString, string const& paramHeader, bool& parameterValue,
                               bool& parameterSetFlag)
    {
        if (parameterSetFlag)
        {
            ostringstream error;
            error<<"The '"<<paramHeader<<"' parameter cannot be set more than once";
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }
        string paramContent = paramString.substr(paramHeader.size());
        if (paramContent == "true")
        {
            parameterValue = true;
        }
        else if (paramContent == "false")
        {
            parameterValue = false;
        }
        else
        {
            ostringstream error;
            error<<"The '"<<paramHeader<<"' parameter must have a value of 'true' or 'false'; '"
                 <<paramContent<<"' is not valid.";
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }
        parameterSetFlag = true;
    }

public:
    /**
     * The operator won't accept more than this number of optional parameters.
     */
    static const size_t MAX_PARAMETERS = 2;

    /**
     * Parse and create the settings; throw an exception if any of the given parameters are not valid.
     * @param operatorParameters the parameters passed to the operator.
     * @param logical true if we are called in the Logical phase, false if in the Physical phase
     * @param query the query context
     */
    InstanceStatsSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                          bool logical,
                          shared_ptr<Query>& query):
       _dumpDataToLog(false),
       _dumpDataToLogSet(false),
       _global(false),
       _globalSet(false)
    {
        string const logParamHeader     = "log=";
        string const globalParamHeader  = "global=";
        size_t nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   /* assert-like exception. Caller should have taken care of this! */
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                  << "illegal number of parameters passed to InstanceStatsSettings";
        }
        for (size_t i= 0; i<nParams; ++i)
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[i];
            string parameterString;

            /* Here we extract data from a constant SciDB parameter. Note the pattern is different based on whether we
             * are in the logical of physical phase. The same pattern applies to constants of other types (DOUBLE).
             */
            if (logical)
            {
                parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->
                                           getExpression(),query, TID_STRING).getString();
            }
            else
            {
                parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->
                                  getExpression()->evaluate().getString();
            }
            /* very simple parsing */
            if (starts_with(parameterString, logParamHeader))
            {
                parseBooleanParameter(parameterString, logParamHeader, _dumpDataToLog, _dumpDataToLogSet);
            }
            else if (starts_with(parameterString, globalParamHeader))
            {
                parseBooleanParameter(parameterString, globalParamHeader, _global, _globalSet);
            }
            else
            {
                ostringstream error;
                error<<"Unrecognized parameter: '"<<parameterString<<"'";
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }
        }
    }

    /**
     * @return true if data should be dumped to the log, false otherwise.
     */
    bool dumpDataToLog() const
    {
        return _dumpDataToLog;
    }

    /**
     * @return true if we should return a global summary, false if per-instance summary.
     */
    bool global() const
    {
        return _global;
    }

};

}

#endif //INSTANCE_STATS_SETTINGS
