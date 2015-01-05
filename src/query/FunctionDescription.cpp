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
 * @file FunctionDescription.cpp
 *
 * @brief Function Description objects. 
 *
 * @author Paul G. Brown <paul_geoffrey_brown@yahoo.com> 
 */
#include <vector>
#include "query/TypeSystem.h"
#include "query/FunctionDescription.h"
#include "query/FunctionLibrary.h"


namespace scidb
{

std::string FunctionDescription::getMangleName() const
{
    std::stringstream ss;
    for (size_t k = 0; k < _outputArgs.size(); k++) {
        if (k != 0) {
            ss << ',';
        }
        ss << _outputArgs[k];
    }
    ss << ' ' << _name << "(";
    for (size_t k = 0; k < _inputArgs.size(); k++) {
        if (k != 0) {
            ss << ',';
        }
        ss << _inputArgs[k];
    }
    ss << ')';
    return ss.str();
}

std::ostream& operator<<(std::ostream& stream,
                         const FunctionDescription& ob) 
{
    stream  << "function " << ob.getName() 
			<< " (" << ob.getInputArgs() << " )"
			<< " returns (" << ob.getOutputArgs() << " )";

    return stream;
}

UserDefinedFunction::UserDefinedFunction(FunctionDescription const& desc)
{
    FunctionLibrary::getInstance()->addFunction(desc);
}

UserDefinedConverter::UserDefinedConverter(TypeId from, TypeId to, ConversionCost cost, FunctionPointer ptr)
{
    FunctionLibrary::getInstance()->addConverter(from, to, ptr, cost);
}


UserDefinedType::UserDefinedType(Type const& type)
{
    TypeLibrary::registerType(type);
}

}
