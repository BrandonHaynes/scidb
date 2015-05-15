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

#include <stdlib.h>
#include <unistd.h>
#include <system/Utils.h>
#include <system/Exceptions.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

void exit(int status)
{
#if !defined(NDEBUG) && defined(CLEAN_EXIT)
    ::exit(status);
#endif
    ::_exit(status);
}

std::string getDir(const std::string& filePath)
{
    size_t found = filePath.find_last_of("/");

    if (found == std::string::npos)
    {
        return ".";
    }

    if (found == 0)
    {
        return "/";
    }

    return filePath.substr(0,found);
}

bool isFullyQualified(const std::string& filePath)
{
    return !filePath.empty() && filePath[0]=='/';
}

FILE* openMemoryStream(char const* ptr,size_t size)
{
    FILE* f = tmpfile();

    if (NULL == f)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,SCIDB_LE_OPERATION_FAILED) << "tmpfile";
    }

    size_t rc = fwrite(ptr,1,size,f);

    if (rc != size)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,SCIDB_LE_OPERATION_FAILED) << "fwrite";
    }

    fseek(f,0,SEEK_SET);
    return f;
}

void bad_dynamic_cast(const std::type_info& b,const std::type_info& d)
{
    std::stringstream s;                                // Formats the message

    s << " safe_dynamic_cast: bad cast from "           // Insert message text
      << b.name()   << " to "                           // ...and source type
      << d.name();                                      // ...and target type

    ASSERT_EXCEPTION(false,s.str());                    // And report failure
}

/****************************************************************************/
}
/****************************************************************************/
