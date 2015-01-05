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
 * @brief Iquery commands classes
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */
 
#ifndef IQUERY_COMMANDS
#define IQUERY_COMMANDS

#include <string>

class IqueryCmd
{
public:
    enum Type
    {
        SET,
        LANG,
        VERBOSE,
        TIMER,
        FETCH,
        HELP,
        QUIT,
        FORMAT,
        BINARY_FORMAT
    };

    IqueryCmd(Type cmdtype):
        _cmdtype(cmdtype)
    {
    }

    Type getCmdType() const
    {
        return _cmdtype;
    }

    virtual ~IqueryCmd()
    {
    }
private:
    Type _cmdtype;
};

class IntIqueryCmd: public IqueryCmd
{
public:
    IntIqueryCmd(Type cmdtype, int value):
        IqueryCmd(cmdtype),
        _value(value)
    {
    }

    int getValue() const
    {
        return _value;
    }

private:
    int _value;
};

class StrIqueryCmd: public IqueryCmd
{
public:
    StrIqueryCmd(Type cmdtype, const std::string& value):
        IqueryCmd(cmdtype),
        _value(value)
    {
    }

    const std::string& getValue() const
    {
        return _value;
    }

private:
    std::string _value;
};
#endif //IQUERY_COMMANDS
