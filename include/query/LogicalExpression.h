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
 * @file LogicalExpression.h
 *
 * @brief Instances of logical expressions
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @author roman.simakov@gmail.com
 */

#ifndef LOGICALEXPRESSION_H_
#define LOGICALEXPRESSION_H_

#include <boost/shared_ptr.hpp>

#include "array/Metadata.h"
#include "array/Array.h"
#include "query/TypeSystem.h"

namespace scidb
{

class ParsingContext;

/**
 * Base class for logical expressions
 */
class LogicalExpression
{
public:
	LogicalExpression(const boost::shared_ptr<ParsingContext>& parsingContext):
		_parsingContext(parsingContext)
	{}

    virtual ~LogicalExpression() {} /**< To make base class virtual */

	boost::shared_ptr<ParsingContext> getParsingContext() const
	{
		return _parsingContext;
	}

    virtual void toString(std::ostream &out, int indent = 0) const;

private:
	boost::shared_ptr<ParsingContext> _parsingContext;
};

class AttributeReference: public LogicalExpression
{
public:
    AttributeReference(const boost::shared_ptr<ParsingContext>& parsingContext,
    	const std::string& arrayName, const std::string& attributeName):
    	LogicalExpression(parsingContext), _arrayName(arrayName), _attributeName(attributeName)
    {
    }

	const std::string& getArrayName() const	{
		return _arrayName;
	}

	const std::string& getAttributeName() const	{
		return _attributeName;
	}

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
    * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString(std::ostream &out, int indent = 0) const;

private:
	std::string _arrayName;
	std::string _attributeName;
};

class Constant : public LogicalExpression
{
public:
	Constant(const boost::shared_ptr<ParsingContext>& parsingContext, const  Value& value,
		const  TypeId& type): LogicalExpression(parsingContext), _value(value), _type(type)
	{
	}

	const  Value& getValue() const {
		return _value;
	}

	const  TypeId& getType() const {
        return _type;
    }


    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
    * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString(std::ostream &str, int indent = 0) const;

private:
	 Value _value;
	 TypeId _type;
};


class Function : public LogicalExpression
{
public:
	Function(const boost::shared_ptr<ParsingContext>& parsingContext, const std::string& function,
		const std::vector<boost::shared_ptr<LogicalExpression> >& args):
		LogicalExpression(parsingContext), _function(function), _args(args)
	{
	}

	const std::string& getFunction() const {
		return _function;
	}

	const std::vector<boost::shared_ptr<LogicalExpression> >& getArgs() const {
		return _args;
	}

    virtual void toString(std::ostream &out, int indent = 0) const;

private:
	std::string _function;
	std::vector<boost::shared_ptr<LogicalExpression> > _args;
};


} // namespace scidb

#endif /* LOGICALEXPRESSION_H_ */
