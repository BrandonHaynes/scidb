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
 * @author: Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Class for controlling built-in and user defined error codes and messages.
 */

#ifndef ERRORSLIBRARYH_
#define ERRORSLIBRARY_H_

#include <string>
#include <map>

#include "util/Singleton.h"

namespace scidb
{

class ErrorsLibrary: public Singleton<ErrorsLibrary>
{
public:
    typedef std::map<int32_t, std::string> ErrorsMessages;

    void registerErrors(const std::string &errorsNamespace, ErrorsMessages* errorsMessages);

    void unregisterErrors(const std::string &errorsNamespace);

    const std::string getShortErrorMessage(int32_t shortError);

    const std::string getLongErrorMessage(const std::string &errorsNamespace, int32_t longError);

private:
    ErrorsLibrary();

    ErrorsMessages _builtinShortErrorsMsg;

    ErrorsMessages _builtinLongErrorsMsg;

    typedef std::map<std::string, ErrorsMessages*> ErrorsNamespaces;
    ErrorsNamespaces _errorNamespaces;

    class Mutex _lock;

    friend class Singleton<ErrorsLibrary>;
};

}
#endif /* ERRORSLIBRARYH_ */
