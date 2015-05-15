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
 * SchemaUtils.h
 *
 *  Created on: Aug 13, 2014
 *      Author: Donghui Zhang
 */

#ifndef SCHEMAUTILS_H_
#define SCHEMAUTILS_H_

#include <boost/shared_ptr.hpp>
#include <array/Array.h>

namespace scidb
{

/**
 * Given an array, it is often needed to access its schema, dimensions and attributes (with or without empty tag.
 * This small utility structure makes it handy to access any of them.
 *
 * @note For performance reason, this struct contains const references to the input array/schema.
 *       It is the caller's responsibility to ensure the input does not go out of scope before SchemaUtils.
 */
struct SchemaUtils
{
    boost::shared_ptr<Array> const& _array;
    ArrayDesc const& _schema;
    Attributes const& _attrsWithET;
    Attributes const& _attrsWithoutET;
    Dimensions const& _dims;
    const size_t _nAttrsWithET;
    const size_t _nAttrsWithoutET;

    SchemaUtils(boost::shared_ptr<Array> const& inputArray)
    : _array(inputArray),
      _schema(inputArray->getArrayDesc()),
      _attrsWithET(inputArray->getArrayDesc().getAttributes(false)),
      _attrsWithoutET(inputArray->getArrayDesc().getAttributes(true)),
      _dims(inputArray->getArrayDesc().getDimensions()),
      _nAttrsWithET(_attrsWithET.size()),
      _nAttrsWithoutET(_attrsWithoutET.size())
    {
    }

    SchemaUtils(ArrayDesc const& schema)
    : _array(boost::shared_ptr<Array>()),
      _schema(schema),
      _attrsWithET(schema.getAttributes(false)),
      _attrsWithoutET(schema.getAttributes(true)),
      _dims(schema.getDimensions()),
      _nAttrsWithET(_attrsWithET.size()),
      _nAttrsWithoutET(_attrsWithoutET.size())
    {
    }
};
/**
 * It is common practice that in implementing some PhysicalOperator's execute() method, some code is extracted to a sub-routine.
 * This structure defines some commonly used variables.
 * The expected use case is that a CommonVariablesInExecute object is defined in execute(), and passed by reference to the sub-routine.
 */
struct CommonVariablesInExecute
{
    boost::shared_ptr<Query> const& _query;
    SchemaUtils const _input;
    SchemaUtils const _output;

    CommonVariablesInExecute(
            boost::shared_ptr<Array> const& inputArray,
            boost::shared_ptr<Array> const& outputArray,
            boost::shared_ptr<Query> const& query
            )
    : _query(query),
      _input(inputArray),
      _output(outputArray)
    {}
};

}

#endif /* SCHEMAUTILS_H_ */
