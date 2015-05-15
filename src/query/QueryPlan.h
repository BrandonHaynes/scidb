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
 * QueryPlan.h
 *
 *  Created on: Dec 24, 2009
 *      Author: Emad, roman.simakov@gmail.com
 */

#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "array/Metadata.h"
#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "system/SystemCatalog.h"


namespace scidb
{

/**
 * Node of logical plan of query. Logical node keeps logical operator to
 * perform inferring result type and validate types.
 */
class LogicalQueryPlanNode
{
public:
    LogicalQueryPlanNode(boost::shared_ptr<ParsingContext>  const&,
                         boost::shared_ptr<LogicalOperator> const&);

    LogicalQueryPlanNode(boost::shared_ptr<ParsingContext>  const&,
                         boost::shared_ptr<LogicalOperator> const&,
                         std::vector<boost::shared_ptr<LogicalQueryPlanNode> > const &children);

    void addChild(const boost::shared_ptr<LogicalQueryPlanNode>& child)
    {
        _childNodes.push_back(child);
    }

    boost::shared_ptr<LogicalOperator> getLogicalOperator()
    {
        return _logicalOperator;
    }

    std::vector<boost::shared_ptr<LogicalQueryPlanNode> >& getChildren()
    {
        return _childNodes;
    }

    bool isDdl() const
    {
        return _logicalOperator->getProperties().ddl;
    }

    bool supportsTileMode() const
    {
        return _logicalOperator->getProperties().tile;
    }

    boost::shared_ptr<ParsingContext> getParsingContext() const
    {
        return _parsingContext;
    }

    const ArrayDesc& inferTypes      (boost::shared_ptr<Query>);
    void             inferArrayAccess(boost::shared_ptr<Query>&);

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &,int indent = 0,bool children = true) const;

  private:
    boost::shared_ptr<LogicalOperator>                    _logicalOperator;
    std::vector<boost::shared_ptr<LogicalQueryPlanNode> > _childNodes;
    boost::shared_ptr<ParsingContext>                     _parsingContext;
};

class PhysicalQueryPlanNode;
typedef boost::shared_ptr<PhysicalOperator>      PhysOpPtr;
typedef boost::shared_ptr<PhysicalQueryPlanNode> PhysNodePtr;

/*
 *  Currently LogicalQueryPlanNode and PhysicalQueryPlanNode have similar structure.
 *  It may change in future as it needed
 */
class PhysicalQueryPlanNode
    : boost::noncopyable,
      public boost::enable_shared_from_this<PhysicalQueryPlanNode>
{
  public:
    PhysicalQueryPlanNode()
    {}

    PhysicalQueryPlanNode(PhysOpPtr const& physicalOperator,
                          bool agg, bool ddl, bool tile);

    PhysicalQueryPlanNode(PhysOpPtr const& PhysicalOperator,
                          std::vector<PhysNodePtr> const& childNodes,
                          bool agg, bool ddl, bool tile);

    virtual ~PhysicalQueryPlanNode() {}

    void addChild(const PhysNodePtr & child)
    {
        child->_parent = shared_from_this();
        _childNodes.push_back(child);
    }

    /**
     * Removes node pointed to by targetChild from children.
     * @param targetChild node to remove. Must be in children.
     */
    void removeChild(const PhysNodePtr & targetChild)
    {
        std::vector<PhysNodePtr> newChildren;
        for(size_t i = 0; i < _childNodes.size(); i++)
        {
            if (_childNodes[i] != targetChild)
            {
                newChildren.push_back(_childNodes[i]);
            }
            else
            {
                targetChild->_parent.reset();
            }
        }
        assert(_childNodes.size() > newChildren.size());
        _childNodes = newChildren;
    }

    /**
     * Replaces targetChild with newChild in children.
     * @param targetChild node to remove. Must be in children.
     * @param newChild node to insert. Must be in children.
     */
    void replaceChild(const PhysNodePtr & targetChild, const PhysNodePtr & newChild)
    {
        bool removed = false;
        std::vector<PhysNodePtr> newChildren;
        for(size_t i = 0; i < _childNodes.size(); i++)
        {
            if (_childNodes[i] != targetChild)
            {
                newChildren.push_back(_childNodes[i]);
            }
            else
            {
                newChild->_parent = shared_from_this();
                newChildren.push_back(newChild);
                removed = true;
            }
        }
        _childNodes = newChildren;
        assert(removed); removed = removed; // Eliminate warnings
    }

    PhysOpPtr getPhysicalOperator()
    {
        return _physicalOperator;
    }

    std::vector<PhysNodePtr>& getChildren()
    {
        return _childNodes;
    }

    bool hasParent() const
    {
        return _parent.lock().get() != NULL;
    }

    void resetParent()
    {
        _parent.reset();
    }

    const PhysNodePtr getParent()
    {
        return _parent.lock();
    }

    bool isAgg() const
    {
        return _agg;
    }

    bool isDdl() const
    {
        return _ddl;
    }

    bool supportsTileMode() const
    {
        return _tile;
    }

    //TODO: there should be a list of arbitrary markers for optimizer to scratch with.
    //Something like a std::map<std::string, boost::any>.

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &str, int indent = 0, bool children = true) const;

    /**
     * Retrieve an ordered list of the shapes of the arrays to be input to this
     * node.
     */
    std::vector<ArrayDesc> getChildSchemas() const
    {
        std::vector<ArrayDesc> result;
        for (size_t i = 0, count = _childNodes.size(); i < count; ++i)
        {
            PhysNodePtr const& child = _childNodes[i];
            result.push_back(child->getPhysicalOperator()->getSchema());
        }
        return result;
    }

    /**
     * Determine if this node is for the PhysicalRepart operator.
     * @return true if physicalOperator is PhysicalRepart. False otherwise.
     */
    bool isRepartNode() const
    {
        return _physicalOperator.get() != NULL &&
               _physicalOperator->getPhysicalName() == "physicalRepart";
    }

    /**
     * Determine if this node is for the PhysicalSG operator.
     * @return true if physicalOperator is PhysicalSG. False otherwise.
     */
    bool isSgNode() const
    {
        return _physicalOperator.get() != NULL &&
                _physicalOperator->getPhysicalName() == "impl_sg";
    }

    bool isStoringSg() const;

    /**
     * @return the sgMovable flag
     */
    bool isSgMovable() const
    {
        return _isSgMovable;
    }

    /**
     * Set the sgMovable flag
     * @param value value to set
     */
    void setSgMovable(bool value)
    {
        _isSgMovable = value;
    }

    /**
     * @return the sgOffsetable flag
     */
    bool isSgOffsetable() const
    {
        return _isSgOffsetable;
    }

    /**
     * Set the sgOffsetable flag
     * @param value value to set
     */
    void setSgOffsetable(bool value)
    {
        _isSgOffsetable = value;
    }

    /**
     * Delegator to physicalOperator.
     */
    bool changesDistribution() const
    {
        return _physicalOperator->changesDistribution(getChildSchemas());
    }

    /**
     * Delegator to physicalOperator.
     */
    bool outputFullChunks() const
    {
        return _physicalOperator->outputFullChunks(getChildSchemas());
    }

    /**
      * [Optimizer API] Determine if the output chunks
      * of this subtree will be completely filled.
      * Optimizer may insert SG operations for subtrees
      * that do not provide full chunks.
      * @return true if output chunking is guraranteed full, false otherwise.
      */
    bool subTreeOutputFullChunks() const
    {
        if (isSgNode())
        {
            return true;
        }
        for (size_t i = 0, count = _childNodes.size(); i< count; ++i)
        {
            if (!_childNodes[i]->subTreeOutputFullChunks())
            {
                return false;
            }
        }
        return _physicalOperator->outputFullChunks(getChildSchemas());
    }

    DistributionRequirement getDistributionRequirement() const
    {
        return _physicalOperator->getDistributionRequirement(getChildSchemas());
    }

    bool needsSpecificDistribution() const
    {
        return getDistributionRequirement().getReqType()== DistributionRequirement::SpecificAnyOrder;
    }

    /**
     * @return the number of attributes emitted by the node.
     */
    double getDataWidth()
    {
        return _boundaries.getSizeEstimateBytes(getPhysicalOperator()->getSchema());
    }

    /**
     * @return stats about distribution of node output
     */
    const ArrayDistribution& getDistribution() const
    {
        return _distribution;
    }

    /**
     * Calculate information about distribution of node output, using
     * the distribution stats of the child nodes, plus
     * the data provided from the PhysicalOperator. Sets distribution
     * stats of node to the result.
     * @param prev distribution stats of previous node's output
     * @return new distribution stats for this node's output
     */
    const ArrayDistribution& inferDistribution ()
    {
        std::vector<ArrayDistribution> childDistros;
        for (size_t i =0; i<_childNodes.size(); i++)
        {
            childDistros.push_back(_childNodes[i]->getDistribution());
        }
        _distribution = _physicalOperator->getOutputDistribution(childDistros, getChildSchemas());
        return _distribution;
    }

    //I see an STL pattern coming soon...
    const PhysicalBoundaries& getBoundaries() const
    {
        return _boundaries;
    }

    const PhysicalBoundaries& inferBoundaries()
    {
        std::vector<PhysicalBoundaries> childBoundaries;
        for (size_t i =0; i<_childNodes.size(); i++)
        {
            childBoundaries.push_back(_childNodes[i]->getBoundaries());
        }
        _boundaries = _physicalOperator->getOutputBoundaries(childBoundaries, getChildSchemas());
        return _boundaries;
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _childNodes;
        ar & _agg;
        ar & _ddl;
        ar & _tile;
        ar & _isSgMovable;
        ar & _isSgOffsetable;
        //We don't need distribution or sizing info - they are used for optimization only.

        /*
         * We not serializing whole operator object, to simplify user's life and get rid work serialization
         * user classes and inherited SciDB classes. Instead this we serializing operator name and
         * its parameters, and later construct operator by hand
         */
        if (Archive::is_loading::value)
        {
            std::string logicalName;
            std::string physicalName;
            PhysicalOperator::Parameters parameters;
            ArrayDesc schema;

            ar & logicalName;
            ar & physicalName;
            ar & parameters;
            ar & schema;

            _physicalOperator = OperatorLibrary::getInstance()->createPhysicalOperator(
                        logicalName, physicalName, parameters, schema);
            _physicalOperator->setTileMode(_tile);
        }
        else
        {
            std::string logicalName = _physicalOperator->getLogicalName();
            std::string physicalName = _physicalOperator->getPhysicalName();
            PhysicalOperator::Parameters parameters = _physicalOperator->getParameters();
            ArrayDesc schema = _physicalOperator->getSchema();

            ar & logicalName;
            ar & physicalName;
            ar & parameters;
            ar & schema;
        }
    }

private:
    PhysOpPtr _physicalOperator;

    std::vector< PhysNodePtr > _childNodes;
    boost::weak_ptr <PhysicalQueryPlanNode> _parent;

    bool _agg;
    bool _ddl;
    bool _tile;

    bool _isSgMovable;
    bool _isSgOffsetable;

    ArrayDistribution _distribution;
    PhysicalBoundaries _boundaries;
};

/**
 * The LogicalPlan represents result of parsing query and is used for validation query.
 * It's input data for optimization and generation physical plan.
 */
class LogicalPlan
{
public:
    LogicalPlan(const boost::shared_ptr<LogicalQueryPlanNode>& root);

    boost::shared_ptr<LogicalQueryPlanNode> getRoot()
    {
        return _root;
    }

    void setRoot(const boost::shared_ptr<LogicalQueryPlanNode>& root)
    {
        _root = root;
    }

    const ArrayDesc& inferTypes(boost::shared_ptr< Query>& query)
    {
        return _root->inferTypes(query);
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        return _root->inferArrayAccess(query);
    }

	/**
	 * Retrieve a human-readable description.
	 * Append a human-readable description of this onto str. Description takes up
	 * one or more lines. Append indent spacer characters to the beginning of
	 * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &str, int indent = 0, bool children = true) const;

private:
    boost::shared_ptr<LogicalQueryPlanNode> _root;
};

/**
 * The PhysicalPlan is produced by Optimizer or in simple cases directly by query processor (DDL).
 * It has ready to execution operator nodes and will be passed to an executor.
 */
class PhysicalPlan
{
public:
    PhysicalPlan(const boost::shared_ptr<PhysicalQueryPlanNode>& root);

    boost::shared_ptr<PhysicalQueryPlanNode> getRoot()
    {
        return _root;
    }

    bool empty() const
    {
        return _root == boost::shared_ptr<PhysicalQueryPlanNode>();    // _root is NULL
    }

    bool isDdl() const
    {
    	assert(!empty());
    	return _root->isDdl();
    }

    bool supportsTileMode() const
    {
    	assert(!empty());
    	return _root->supportsTileMode();
    }

	void setRoot(const boost::shared_ptr<PhysicalQueryPlanNode>& root)
	{
		_root = root;
	}

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &out, int indent = 0, bool children = true) const;

private:
    boost::shared_ptr<PhysicalQueryPlanNode> _root;
};

typedef boost::shared_ptr<PhysicalPlan> PhysPlanPtr;

} // namespace


#endif /* QUERYPLAN_H_ */
