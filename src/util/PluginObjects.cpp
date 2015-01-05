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
 * @file PluginObjects.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief A collection of plugin objects.
 *
 * Use this class to collect information about objects of plugin. One instance per object kind.
 */

#include <vector>

#include "util/PluginManager.h"
#include "util/PluginObjects.h"

using namespace std;

namespace scidb
{

void PluginObjects::addObject(const std::string& objectName)
{
    string loadingLibrary =
#ifndef SCIDB_CLIENT
            PluginManager::getInstance()->loadingLibrary()
#else
            "client"
#endif
            ;
    if (loadingLibrary.empty())
        loadingLibrary = "scidb";
    _objects[objectName] = loadingLibrary;
}

const std::string& PluginObjects::getObjectLibrary(const std::string& objectName) const
{
    map< string, string>::const_iterator i = _objects.find(objectName);
    if (i != _objects.end())
        return i->second;
    else
        throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR, SCIDB_LE_OPERATOR_NOT_FOUND) << objectName;
}

}
