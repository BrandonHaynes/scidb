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
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief This operator shows parameters of other operator
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/MemArray.h"

using namespace std;

namespace scidb
{

class PhysicalHelp: public PhysicalOperator
{
public:
    PhysicalHelp(const std::string& logicalName,
        const std::string& physicalName, const Parameters& parameters,
        const ArrayDesc& schema) :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution>& inputDistributions,
                                                 const std::vector< ArrayDesc>& inputSchemas) const
    {
        return ArrayDistribution(psLocalInstance);
    }

    void preSingleExecute(boost::shared_ptr<Query> query)
    {
        stringstream ss;

        if (_parameters.size() == 1)
        {
            const string opName =
                ((boost::shared_ptr<OperatorParamPhysicalExpression>&) _parameters[0])->getExpression()->evaluate().getString();
            boost::shared_ptr<LogicalOperator> op =
                OperatorLibrary::getInstance()->createLogicalOperator(opName);
            ss << "Operator: " << opName << endl << "Usage: ";

            if (op->getUsage() == "")
            {

                ss << opName << "(";

                bool first = true;
                for (OperatorParamPlaceholders::const_iterator it =
                    op->getParamPlaceholders().begin();
                    it != op->getParamPlaceholders().end(); ++it)
                {
                    if (!first)
                        ss << ", ";
                    switch ((*it)->getPlaceholderType())
                    {
                        case PLACEHOLDER_INPUT:
                            ss << "<input>";
                            break;

                        case PLACEHOLDER_ARRAY_NAME:
                            ss << "<array name>";
                            break;

                        case PLACEHOLDER_ATTRIBUTE_NAME:
                            ss << "<attribute name>";
                            break;

                        case PLACEHOLDER_CONSTANT:
                            ss << "<constant>";
                            break;

                        case PLACEHOLDER_DIMENSION_NAME:
                            ss << "<dimension name>";
                            break;

                        case PLACEHOLDER_EXPRESSION:
                            ss << "<expression>";
                            break;

                        case PLACEHOLDER_SCHEMA:
                            ss << "<schema>";
                            break;

                        case PLACEHOLDER_AGGREGATE_CALL:
                            ss << "<aggregate call>";
                            break;

                        case PLACEHOLDER_VARIES:
                            ss << "...";
                            break;

                        default:
                            assert(0);
                    }

                    first = false;
                }

                ss << ")";
            }
            else
            {
                ss << op->getUsage();
            }
        }
        else
        {
            ss
                << "Use existing operator name as argument for help operator. You can see all operators by executing list('operators').";
        }

        _result = boost::shared_ptr<MemArray>(new MemArray(_schema,query));
        boost::shared_ptr<ArrayIterator> arrIt = _result->getIterator(0);
        Coordinates coords;
        coords.push_back(0);
        Chunk& chunk = arrIt->newChunk(coords);
        boost::shared_ptr<ChunkIterator> chunkIt = chunk.getIterator(query);
        Value v(TypeLibrary::getType(TID_STRING));
        v.setString(ss.str().c_str());
        chunkIt->writeItem(v);
        chunkIt->flush();
    }

    boost::shared_ptr<Array> execute(
        std::vector<boost::shared_ptr<Array> >& inputArrays,
        boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        if (!_result) {
            _result = boost::shared_ptr<MemArray>(new MemArray(_schema,query));
        }
        return _result;
    }

private:
    boost::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalHelp, "help", "impl_help")

} //namespace
