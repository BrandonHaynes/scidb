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
 * OperatorLibrary.h
 *
 *  Created on: Feb 11, 2010
 *      Author: roman.simakov@gmail.com
 */

#ifndef OPERATORLIBRARY_H_
#define OPERATORLIBRARY_H_

#include <map>
#include <vector>
#include <string>
#include <boost/shared_ptr.hpp>

#include "query/Operator.h"
#include "util/Singleton.h"
#include "util/StringUtil.h"


namespace scidb
{

/**
 * The class is entry point to operators library and will create every known for it operator.
 * In order to create operator it must have object factory class and possibility to register it
 * in library. It's implemented in cpp file.
 */
class OperatorLibrary: public Singleton<OperatorLibrary>
{
public:
    OperatorLibrary();

    // @return logical operator by its name or NULL if not found
    boost::shared_ptr<LogicalOperator> createLogicalOperator(const std::string& logicalName,
            const std::string& alias = "");

    // @return a physical operator for given logical and physical operator name
    boost::shared_ptr<PhysicalOperator> createPhysicalOperator(const std::string& logicalName,
            const std::string& physicalName, const PhysicalOperator::Parameters& parameters, const ArrayDesc& schema);

    // @return a vector of physical operators for given logical operator name
    void createPhysicalOperators(const std::string& logicalName,
            std::vector< boost::shared_ptr<PhysicalOperator> >& physicalOperators,
            const PhysicalOperator::Parameters& parameters, const ArrayDesc& schema);

    // Add new logical operator factory
    void addLogicalOperatorFactory(BaseLogicalOperatorFactory*);

    // Add new physical operator factory
    void addPhysicalOperatorFactory(BasePhysicalOperatorFactory*);

    void getPhysicalNames(const std::string& logicalName, std::vector<std::string>& physicalOperatorsNames);
    void getLogicalNames(std::vector<std::string>& logicalOperatorsNames);

    bool hasLogicalOperator(const std::string &logicalOperatorName);

    const PluginObjects& getOperatorLibraries() {
        return _operatorLibraries;
    }

private:
    typedef std::map<std::string, BasePhysicalOperatorFactory* > PhysicalOperatorFactories;
    typedef std::pair<std::string, BasePhysicalOperatorFactory* > PhysicalOperatorFactoriesPair;
    typedef std::map<std::string, BaseLogicalOperatorFactory* > LogicalOperatorFactories;
    typedef std::map<std::string, BaseLogicalOperatorFactory*, __lesscasecmp> LogicalOperatorFactoriesMap;
    typedef std::map<std::string, PhysicalOperatorFactories, __lesscasecmp> PhysicalOperatorFactoriesMap;
    LogicalOperatorFactoriesMap _logicalOperatorFactories;
    PhysicalOperatorFactoriesMap _physicalOperatorFactories;
    PluginObjects _operatorLibraries;
};


} // namespace

#endif /* OPERATORLIBRARY_H_ */
