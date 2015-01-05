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

/****************************************************************************/

#include <query/Parser.h>                                // For loadModule
#include <query/Operator.h>                              // For PhysicalOperator
#include <system/Resources.h>                            // For fileExists

/****************************************************************************/
namespace scidb { namespace {
/****************************************************************************/
using namespace std;                                     // For all of std
using namespace boost;                                   // For all of boost
/****************************************************************************/

struct PhysicalLoadModule : PhysicalOperator
{
    PhysicalLoadModule(const string& l,const string& p,const Parameters& a,const ArrayDesc& s)
       : PhysicalOperator(l,p,a,s)
    {}

    shared_ptr<Array>
    execute(vector<shared_ptr<Array> >&,shared_ptr<Query> query)
    {
        if (query->isCoordinator())                      // Are we a coordinator?
        {
            string path(((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString());

            if (!Resources::getInstance()->fileExists(path,0,query))
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR,SCIDB_LE_FILE_NOT_FOUND) << path;
            }

            loadModule(path);                            // ...load user module
        }

        return shared_ptr<Array>();                      // Nothing to return
    }
};

/****************************************************************************/
} DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalLoadModule,"load_module","impl_load_module")}
/****************************************************************************/
