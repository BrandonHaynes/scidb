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
 * @file
 *
  * @brief Platform specific routines
 */

#ifndef SYSTEMUTILS_H_
#define SYSTEMUTILS_H_

#include <string>
#include <stdio.h>
#include <assert.h>
#include <iosfwd>
#include <boost/filesystem.hpp>
#include "log4cxx/logger.h"

namespace scidb
{
    /**
     * The macro is used to avoid compile-time warning.
     * @note
     *   Sample usage is:
     *   bool result = ...;
     *   SCIDB_ASSERT(result);
     *
     */
    #define SCIDB_ASSERT(_cond_) do { size_t _rc_ = sizeof(_cond_); assert(bool(_cond_)); _rc_ = _rc_; /* avoid compiler warning */ } while(0)

    /**
     * Debug-build only: halt the execution of the current thread in a tight loop
     * while a file exists in the filesystem. This function can be inserted into hard-to-reach
     * code paths (system initialization, hard-to-reach functions) with a known file name.
     * You can then create the particular file name on the system (i.e. 'touch /tmp/pause_on_init').
     * You can then run scidb and it will halt upon entering this function in a while-loop.
     * You can then attach a debugger, remove the file, examine the memory, etc.
     * @param filePath the absolute path for the file marker to halt on
     * @param logger optional loger so the function can print to scidb log when engaged
     */
    inline void debugSpinFile(std::string const& filePath, log4cxx::LoggerPtr* logger = 0)
    {
#ifndef NDEBUG
        while(boost::filesystem::exists(filePath))
        {
            if (logger)
            {
                LOG4CXX_DEBUG((*logger), "Spinning on the existence of file "<<filePath);
            }
            sleep(1);
        }
#endif
    }


    /**
     * Terminate process with a given status code.
     * @status the code returned to the OS upon process exit
     * @return does not return
     */
    void exit(int status);

    /**
     *
     * @return the directory part of the file path without the trailing slash
     */
    std::string getDir(const std::string& filePath);

    /**
     *
     * @return true if filePath is considered fully qualified
     */
    bool isFullyQualified(const std::string& filePath);

    /**
     * Open input file stream for specified region of memory
     * @param ptr memory buffer address
     * @param size size of memory buffer
     * @return input file stream (can be used in any read stdio operations)
     */
    FILE* openMemoryStream(char const* ptr, size_t size);
}

#endif //SYSTEM_H_
