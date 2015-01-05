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
 * Optimizer.h
 *
 *  Created on: Dec 24, 2009
 *      Author: Emad, roman.simakov@gmail.com
 */

#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include <iostream>
#include <string>
#include <vector>
#include <map>

#include <boost/shared_ptr.hpp>

#include "system/Config.h"
#include "query/QueryPlan.h"
#include "system/SciDBConfigOptions.h"
#include "query/OperatorLibrary.h"

namespace scidb
{

/**
 * The abstract class for optimization. Inherit this class to
 * implement some optimization.
 * To use optimizer you must get pointer to its interface. This should be done
 * by calling  create() method. It selects implementation of optimizer
 * according to parameter in configuration.
 */
class Optimizer : boost::noncopyable
{
protected:
    /**
     *  Helper function for constructing optimizer implementation without
     *  including headers. Each function must be implemented in related .cpp file
     *  with optimizer implementation.
     */
    virtual boost::shared_ptr<LogicalQueryPlanNode> logicalRewriteIfNeeded(const boost::shared_ptr<Query>& query,
                                                                            boost::shared_ptr< LogicalQueryPlanNode> node);

  public:
    virtual ~Optimizer() {}
    /**
     * This method get logical plan and generate pair of physical plan to be executed
     * and the rest of logical plan that must be optimized only after execution
     * returned physical plan. Note: we should not return a vector of physical fragments
     * immediately.
     *
     * @param query the query whose plan to be optimized
     * @param[in out] logicalPlan is a logical plan to be optimized. After optimization it contains
     * the rest of logical plan to be optimized later.
     * @return physical plan to be executed.
     *
     */
    virtual boost::shared_ptr<PhysicalPlan> optimize(const boost::shared_ptr<Query>& query,
                                                      boost::shared_ptr< LogicalPlan>& logicalPlan) = 0;

    static boost::shared_ptr<Optimizer> create();
};

} // namespace
#endif /* OPTIMIZER_H_ */
