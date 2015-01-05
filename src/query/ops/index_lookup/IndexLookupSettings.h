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
 * @file IndexLookupSettings.h
 * The settings structure for the index_lookup operator.
 * @see InstanceStatsSettings.h
 * @author apoliakov@paradigm4.com
 */

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>

#ifndef INDEX_LOOKUP_SETTINGS
#define INDEX_LOOKUP_SETTINGS

namespace scidb
{

/*
 * Settings for the IndexLookup operator.
 */
class IndexLookupSettings
{
private:
    ArrayDesc const& _inputSchema;
    ArrayDesc const& _indexSchema;
    AttributeID const _inputAttributeId;
    string const _inputAttributeName;
    string _outputAttributeName;
    bool _outputAttributeNameSet;
    size_t _memoryLimit;
    bool _memoryLimitSet;
    bool _indexSorted;
    bool _indexSortedSet;

    void parseMemoryLimit(string const& parameterString, string const& paramHeader)
    {
        if(_memoryLimitSet)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANNOT_BE_SET_MORE_THAN_ONCE) << paramHeader;
        }
        string paramContent = parameterString.substr(paramHeader.size());
        trim(paramContent);
        int64_t sval;
        try
        {
            sval = lexical_cast<int64_t> (paramContent);
        }
        catch (bad_lexical_cast const& exn)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANNOT_PARSE_INTEGER_PARAMETER) << parameterString;
        }
        if (sval <= 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_PARAMETER_NOT_POSITIVE_INTEGER) << parameterString;
        }
        _memoryLimit = sval * MiB;
        _memoryLimitSet = true;
    }

     void parseIndexSorted(string const& parameterString, string const& paramHeader)
     {
         if(_indexSortedSet)
         {
             throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANNOT_BE_SET_MORE_THAN_ONCE) << paramHeader;
         }
         string paramContent = parameterString.substr(paramHeader.size());
         trim(paramContent);
         bool bval;
         //XXX: we need our parser to support named parameters natively!!
         //all of this code is more or less throwaway until that happens
         //thankfully, this pattern is only used for more obscure settings that rarely surface
         istringstream stream(paramContent);
         stream >> std::boolalpha >> bval;
         if (!stream.good())
         {
             throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANNOT_PARSE_BOOLEAN_PARAMETER) << parameterString;
         }
         _indexSorted = bval;
         _indexSortedSet = true;
     }

    void setOutputAttributeName(shared_ptr<OperatorParam>const& param)
    {
        if(_outputAttributeNameSet)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANNOT_BE_SET_MORE_THAN_ONCE) << "output attribute name";
        }
        //extracting the identifier that's been parsed and prepared by scidb
        _outputAttributeName = ((shared_ptr<OperatorParamReference> const&)param)->getObjectName();
        _outputAttributeNameSet = true;
    }

    void checkInputSchemas()
    {
        //throw an error if the schemas don't satisfy us
        ostringstream err;
        if (_indexSchema.getDimensions().size() > 1 ||
            _indexSchema.getAttributes(true).size() > 1)
        {
            //note: index does NOT have to start at 0.
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_IMPROPER_INDEX_SHAPE);
        }
        AttributeDesc const& inputAttribute = _inputSchema.getAttributes()[_inputAttributeId];
        AttributeDesc const& indexAttribute = _indexSchema.getAttributes()[0];
        if (inputAttribute.getType() != indexAttribute.getType())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ATTRIBUTES_DO_NOT_MATCH_TYPES)
                    << _inputAttributeName
                    << inputAttribute.getType()
                    << indexAttribute.getName()
                    << indexAttribute.getType();
        }
        if (indexAttribute.isNullable())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ATTRIBUTE_CANNOT_BE_NULLABLE) <<indexAttribute.getName();
        }
    }

public:
    static const size_t MAX_PARAMETERS = 4;

    IndexLookupSettings(ArrayDesc const& inputSchema,
                        ArrayDesc const& indexSchema,
                        vector<shared_ptr<OperatorParam> > const& operatorParameters,
                        bool logical,
                        shared_ptr<Query>& query):
        _inputSchema            (inputSchema),
        _indexSchema            (indexSchema),
        _inputAttributeId       (dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getObjectNo()),
        _inputAttributeName     (dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getObjectName()),
        _outputAttributeName    (_inputSchema.getAttributes()[_inputAttributeId].getName() + "_index"),
        _outputAttributeNameSet (false),
        _memoryLimit            (Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD) * MiB),
        _memoryLimitSet         (false),
        _indexSorted            (false),
        _indexSortedSet         (false)

    {
        if (dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getInputNo() != 0)
        {   //can happen if user specifies the index attribute!
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_NOT_AN_ATTRIBUTE_IN_INPUT) << _inputAttributeName;
        }

        checkInputSchemas();
        string const memLimitHeader    = "memory_limit=";
        string const indexSortedHeader = "index_sorted=";
        size_t nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                  << "illegal number of parameters passed to UniqSettings";
        }
        for (size_t i = 1; i<nParams; ++i) //parameter 0 is already checked above
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[i];
            if (param->getParamType()== PARAM_ATTRIBUTE_REF)
            {
                setOutputAttributeName(param);
            }
            else
            {
                string parameterString;
                if (logical)
                {
                    parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->
                                               getExpression(),query, TID_STRING).getString();
                }
                else
                {
                    parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->
                                      getExpression()->evaluate().getString();
                }
                if (starts_with(parameterString, memLimitHeader))
                {
                    parseMemoryLimit(parameterString, memLimitHeader);
                }
                else if (starts_with(parameterString, indexSortedHeader))
                {
                    parseIndexSorted(parameterString, indexSortedHeader);
                }
                else
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNRECOGNIZED_PARAMETER) << parameterString;
                }
            }
        }
    }

    /**
     * @return the memory limit (converted to bytes)
     */
    size_t getMemoryLimit() const
    {
        return _memoryLimit;
    }

    /**
     * @return the name of the output attribute
     */
    string const& getOutputAttributeName() const
    {
        return _outputAttributeName;
    }

    /**
     * @return the id of the input attribute
     */
    AttributeID getInputAttributeId() const
    {
        return _inputAttributeId;
    }

    /**
     * @return true if the user claims the index array is already dense and sorted,
     * false otherwise (default).
     */
    bool isIndexPreSorted() const
    {
        return _indexSorted;
    }
};

}

#endif //INDEX_LOOKUP_SETTINGS
