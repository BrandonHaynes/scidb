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

#include <query/Operator.h>

/****************************************************************************/
namespace scidb { namespace {
/****************************************************************************/
/**
 * @brief The operator: load_module().
 *
 * @par Synopsis:
 *   load_module( module )
 *
 * @par Summary:
 *   Loads a SciDB module.
 *
 * @par Input:
 *   - module: the path name of the module file to load.
 *
 * @par Output array:
 *   - NULL
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 */
/****************************************************************************/
using namespace std;                                     // For all of std
using namespace boost;                                   // For all of boost
/****************************************************************************/

struct LogicalLoadModule : LogicalOperator
{
    LogicalLoadModule(const string& n,const string& a)
     : LogicalOperator(n,a)
    {
        ADD_PARAM_CONSTANT("string");                    // Requires a string
        _usage = "load_module(module-path : string)";    // The usage string
    }

    ArrayDesc inferSchema(vector<ArrayDesc>,shared_ptr<Query>)
    {
        return ArrayDesc("load_module",
               Attributes(1,AttributeDesc(0,"module",TID_STRING,0,0)),
               Dimensions(1,DimensionDesc("i",0,0,0,0,1,0)));
    }
};

/****************************************************************************/
} DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalLoadModule,"load_module")}
/****************************************************************************/
