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
 * @file ArrayWriter.h
 *
 *  Created on: 19.02.2010
 *      Author: knizhnik@garret.ru
 *   Rewritten: 29.05.2014 by mjl@paradigm4.com
 * Description: Write out arrays in various formats.
 */

#ifndef ARRAY_WRITER_H_
#define ARRAY_WRITER_H_

#include <stddef.h>
#include <string>

#include "array/Array.h"

namespace scidb
{

    class ArrayWriter
    {
      public:
        enum { DEFAULT_PRECISION = 6 };

        /// Values for #save method 'flags' parameter.
        enum Flags {
            F_APPEND = 0x01,    ///< Open file in append mode
            F_PARALLEL = 0x02   ///< This is a parallel save
        };

        /**
         * Save data from in text format in specified file
         * @param arrayName name of the array which data will be saved
         * @param file path to the exported data file
         * @param query doing the save
         * @param format output format: csv, csv+, tsv, tsv+, sparse, auto, etc.
         * @param flags see ArrayWriter::Flags
         * @return number of saved tuples
         */
       static uint64_t save(std::string const& arrayName,
                            std::string const& file,
                            const boost::shared_ptr<Query>& query,
                            std::string const& format = "auto",
                            unsigned flags = 0);


        /**
         * Save data from in text format in specified file
         * @param array array to be saved
         * @param file path to the exported data file
         * @param query doing the save
         * @param format output format: csv, csv+, tsv, tsv+, sparse, auto, etc.
         * @param flags see ArrayWriter::Flags
         * @return number of saved tuples
         */
        static uint64_t save(Array const& array,
                             std::string const& file,
                             const boost::shared_ptr<Query>& query,
                             std::string const& format = "auto",
                             unsigned flags = 0);


        /**
         * Return the number of digits' precision used to format output.
         * @return number of digits
         */
        static int getPrecision()
        {
            return _precision;
        }

        /**
         * Set the number of digits' precision used to format output.
         * @param prec precision value, or < 0 to restore default
         * @return previous precision value
         */
        static int setPrecision(int prec)
        {
            int prev = _precision;
            _precision = (prec < 0) ? DEFAULT_PRECISION : prec;
            return prev;
        }

        /**
         * @brief Test whether the named format is supported.
         *
         * @description If the format is supported, return a pointer
         * to its canonical name (i.e. its lowercase name), otherwise
         * NULL.  This routine only knows about concrete formats, not
         * about things like "auto" or empty string picking a default
         * format.
         *
         * @par
         * Template formats are recognized but have no canonical name,
         * so fmt.c_str() is returned.  They beginning with '(' and
         * are associated with a custom plugin, e.g. "(myformat)".
         *
         * @see TemplateParser
         *
         * @param fmt the name of the format
         * @retval NULL this format is not supported
         * @retval !NULL format is supported, and this is its canonical name
         */
        static const char* isSupportedFormat(const std::string& fmt);

    private:
        static int _precision;

    };
}

#endif
