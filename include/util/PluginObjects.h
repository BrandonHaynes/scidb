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

#ifndef PLUGINOBJECTS_H
#define PLUGINOBJECTS_H

namespace scidb
{

class PluginObjects
{
public:
    void addObject(const std::string& objectName);
    const std::string& getObjectLibrary(const std::string& objectName) const;

private:
    std::map<std::string, std::string> _objects;
};

}

#endif // PLUGINOBJECTS_H
