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

/**
 * @file QueryTree.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>

#include "log4cxx/logger.h"
#include "query/QueryPlanUtilites.h"
#include "query/QueryPlan.h"
#include "query/LogicalExpression.h"

using namespace boost;
using namespace std;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

// LogicalQueryPlanNode
LogicalQueryPlanNode::LogicalQueryPlanNode(
	const boost::shared_ptr<ParsingContext>& parsingContext,
	const boost::shared_ptr<LogicalOperator>& logicalOperator):
	_logicalOperator(logicalOperator),
	_parsingContext(parsingContext)
{

}

LogicalQueryPlanNode::LogicalQueryPlanNode(
	const boost::shared_ptr<ParsingContext>& parsingContext,
	const boost::shared_ptr<LogicalOperator>& logicalOperator,
	const std::vector<boost::shared_ptr<LogicalQueryPlanNode> > &childNodes):
	_logicalOperator(logicalOperator),
	_childNodes(childNodes),
	_parsingContext(parsingContext)
{
}

const ArrayDesc& LogicalQueryPlanNode::inferTypes(boost::shared_ptr< Query> query)
{
    std::vector<ArrayDesc> inputSchemas;
    ArrayDesc outputSchema;
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        inputSchemas.push_back(_childNodes[i]->inferTypes(query));
    }
    outputSchema = _logicalOperator->inferSchema(inputSchemas, query);
    //FIXME: May be cover inferSchema method with another one and assign alias there?
    if (!_logicalOperator->getAliasName().empty())
    {
        outputSchema.addAlias(_logicalOperator->getAliasName());
    }
    _logicalOperator->setSchema(outputSchema);
    LOG4CXX_DEBUG(logger, "Inferred schema for operator " <<
                  _logicalOperator->getLogicalName() << ": " << outputSchema);
    return _logicalOperator->getSchema();
}

void LogicalQueryPlanNode::inferArrayAccess(boost::shared_ptr<Query>& query)
{
    //XXX TODO: consider non-recursive implementation
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        _childNodes[i]->inferArrayAccess(query);
    }
    assert(_logicalOperator);
    _logicalOperator->inferArrayAccess(query);
}

void LogicalQueryPlanNode::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lInstance] children "<<_childNodes.size()<<"\n";
    _logicalOperator->toString(out,indent+1);

    if (children) {
        for (size_t i = 0; i< _childNodes.size(); i++)
        {
            _childNodes[i]->toString(out, indent+1);
        }
    }
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const boost::shared_ptr<PhysicalOperator>& physicalOperator,
                                             bool agg, bool ddl, bool tile)
: _physicalOperator(physicalOperator),
  _parent(), _agg(agg), _ddl(ddl), _tile(tile), _isSgMovable(true), _isSgOffsetable(true), _distribution()
{
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const boost::shared_ptr<PhysicalOperator>& physicalOperator,
		const std::vector<boost::shared_ptr<PhysicalQueryPlanNode> > &childNodes,
                                             bool agg, bool ddl, bool tile):
	_physicalOperator(physicalOperator),
	_childNodes(childNodes),
    _parent(), _agg(agg), _ddl(ddl), _tile(tile), _isSgMovable(true), _isSgOffsetable(true), _distribution()
{
}

void PhysicalQueryPlanNode::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);

    out<<"[pNode] "<<_physicalOperator->getPhysicalName()<<" agg "<<isAgg()<<" ddl "<<isDdl()<<" tile "<<supportsTileMode()<<" children "<<_childNodes.size()<<"\n";
    _physicalOperator->toString(out,indent+1);

    if (children) {
        out << prefix(' ');
        out << "output full chunks: ";
        out << (outputFullChunks() ? "yes" : "no");
        out << "\n";
        out << prefix(' ');
        out << "changes dstribution: ";
        out << (changesDistribution() ? "yes" : "no");
        out << "\n";
    }

    out << prefix(' ');
    out<<"props sgm "<<_isSgMovable<<" sgo "<<_isSgOffsetable<<"\n";
    out << prefix(' ');
    out<<"diout "<<_distribution<<"\n";
    const ArrayDesc& schema = _physicalOperator->getSchema();
    out << prefix(' ');
    out<<"bound "<<_boundaries
      <<" cells "<<_boundaries.getNumCells();

    if (_boundaries.getStartCoords().size() == schema.getDimensions().size()) {
        out  <<" chunks "<<_boundaries.getNumChunks(schema.getDimensions())
            <<" est_bytes "<<_boundaries.getSizeEstimateBytes(schema)
           <<"\n";
    }
    else {
        out <<" [improperly initialized]\n";
    }

    if (children) {
        for (size_t i = 0; i< _childNodes.size(); i++) {
            _childNodes[i]->toString(out, indent+1);
        }
    }
}

bool PhysicalQueryPlanNode::isStoringSg() const
{
    if ( isSgNode() )
    {
        PhysicalOperator::Parameters params = _physicalOperator->getParameters();
        if (params.size() == 3)
        {
            return true;
        }
        if (params.size() >= 4)
        {
            bool storeResult = ((boost::shared_ptr<OperatorParamPhysicalExpression>&) params[3])->getExpression()->evaluate().getBool();
            return storeResult;
        }
    }
    return false;
}

// LogicalPlan
LogicalPlan::LogicalPlan(const boost::shared_ptr<LogicalQueryPlanNode>& root):
        _root(root)
{

}

void LogicalPlan::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lPlan]:\n";
    _root->toString(out, indent+1, children);
}

// PhysicalPlan
PhysicalPlan::PhysicalPlan(const boost::shared_ptr<PhysicalQueryPlanNode>& root):
        _root(root)
{

}

void PhysicalPlan::toString(std::ostream &out, int const indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[pPlan]:";
    if (_root.get() != NULL)
    {
        out << "\n";
        _root->toString(out, indent+1, children);
    }
    else
    {
        out << "[NULL]\n";
    }
}

} // namespace
