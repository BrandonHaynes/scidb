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
 * @file ra_decl.cpp
 *
 * @author knizhnik@garretr.ru
 *
 * @brief Shared library that loads into SciDB a RA/DECL data types. 
 */

#include <boost/assign.hpp>

#include "query/Operator.h"
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"
#include "system/ErrorsLibrary.h"

using namespace std;
using namespace scidb;
using namespace boost::assign;

#define MIN_RA     0
#define MAX_RA   360

#define MIN_DECL -90
#define MAX_DECL  90

enum
{
    RA_DECL_ERROR1 = SCIDB_USER_ERROR_CODE_START,//RA should be in range [0..360)
    RA_DECL_ERROR2 //RA should be in range [-90..90)
};

static class RaDeclLibrary
{
public:
    RaDeclLibrary()
    {
        _errors[RA_DECL_ERROR1] = "RA should be in range [0..360)";
        _errors[RA_DECL_ERROR2] = "RA should be in range [-90..90)";
        scidb::ErrorsLibrary::getInstance()->registerErrors("ra_decl", &_errors);
    }

    ~RaDeclLibrary()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors("ra_decl");
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _errors;
} _instance;

void RAToDouble(const Value** args, Value* res, void*)
{
    if (args[0]->getDouble() < MIN_RA || args[0]->getDouble() >= MAX_RA)
        throw PLUGIN_USER_EXCEPTION("ra_decl", SCIDB_SE_UDO, RA_DECL_ERROR1);
    res->setDouble(args[0]->getDouble());
}

void DECLToDouble(const Value** args, Value* res, void*)
{
    if (args[0]->getDouble() < MIN_DECL || args[0]->getDouble() >= MAX_DECL)
        throw PLUGIN_USER_EXCEPTION("ra_decl", SCIDB_SE_UDO, RA_DECL_ERROR2);
    res->setDouble(args[0]->getDouble());
}

void RAToOrdinal(const Value** args, Value* res, void*)
{
    if (args[0]->getDouble() < MIN_RA || args[0]->getDouble() >= MAX_RA)
        throw PLUGIN_USER_EXCEPTION("ra_decl", SCIDB_SE_UDO, RA_DECL_ERROR1);
    res->setInt64(int64_t(args[0]->getDouble()*100000));
//    res.setInt64(args[1].getInt64() + int64_t((args[0].getDouble() - MIN_RA)/(MAX_RA-MIN_RA)*args[2].getInt64()));
}

void RAFromOrdinal(const Value** args, Value* res, void*)
{
    res->setDouble(double(args[0]->getInt64())/100000);
//    res.setDouble(MIN_RA + double(args[0].getInt64() - args[1].getInt64()) / args[2].getInt64() * (MAX_RA-MIN_RA));
}

void DECLToOrdinal(const Value** args, Value* res, void*)
{
    if (args[0]->getDouble() < MIN_DECL || args[0]->getDouble() >= MAX_DECL)
        throw PLUGIN_USER_EXCEPTION("ra_decl", SCIDB_SE_UDO, RA_DECL_ERROR2);
   res->setInt64(int64_t(args[0]->getDouble()*100000));
//    res.setInt64(args[1].getInt64() + int64_t((args[0].getDouble() - MIN_DECL)/(MAX_DECL-MIN_DECL)*args[2].getInt64()));
}

void DECLFromOrdinal(const Value** args, Value* res, void*)
{
    res->setDouble(double(args[0]->getInt64())/100000);
//    res.setDouble(MIN_DECL + double(args[0].getInt64() - args[1].getInt64()) / args[2].getInt64() * (MAX_DECL-MIN_DECL));
}

REGISTER_SUBTYPE(right_ascention, 8, TID_DOUBLE);
REGISTER_SUBTYPE(declination, 8, TID_DOUBLE);

/* 
REGISTER_FUNCTION(ordinal, ("right_ascention", TID_INT64, TID_INT64), TID_INT64, RAToOrdinal);
REGISTER_FUNCTION(right_ascention, (TID_INT64, TID_INT64, TID_INT64), "right_ascention", RAFromOrdinal);
REGISTER_FUNCTION(ordinal, ("declination", TID_INT64, TID_INT64), TID_INT64, DECLToOrdinal);
REGISTER_FUNCTION(declination, (TID_INT64, TID_INT64, TID_INT64), "declination", DECLFromOrdinal);
*/

REGISTER_FUNCTION(ordinal, list_of("right_ascention"), TID_INT64, RAToOrdinal);
REGISTER_FUNCTION(right_ascention, list_of(TID_INT64), "right_ascention", RAFromOrdinal);
REGISTER_FUNCTION(ordinal, list_of("declination"), TID_INT64, DECLToOrdinal);
REGISTER_FUNCTION(declination, list_of(TID_INT64), "declination", DECLFromOrdinal);

//
// Need casts to double().
// 
REGISTER_FUNCTION(double, list_of("right_ascention"), TID_DOUBLE, RAToDouble);
REGISTER_FUNCTION(double, list_of("declination"), TID_DOUBLE, DECLToDouble);

