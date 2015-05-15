/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2014-2014 SciDB, Inc.
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

#ifndef CSV_PARSER_H_
#define CSV_PARSER_H_

#include <system/Constants.h>

#include <csv.h>
#include <log4cxx/logger.h>
#include <boost/noncopyable.hpp>

#include <deque>
#include <string>
#include <vector>

namespace scidb {

/**
 *  @brief      A wrapper around the LGPLv2 libcsv parser.
 *
 *  @details    The libcsv parser is awesome, but its callback-driven
 *              API makes it hard to use in some circumstances.  This
 *              class "buffers up" the results of the callbacks,
 *              returning fields and rows when higher software layers
 *              ask for them rather than when libcsv decides to invoke
 *              a callback.  (For those familiar with XML parsers, this
 *              is analogous to converting a SAX-like API into a
 *              DOM-like API.)
 *
 *  @note       Unfortunately, all the input data must be copied in
 *              order to avoid pointer invalidations due to
 *              libcsv-internal realloc(3) calls.  Oh well.
 *
 *  @author     mjl@paradigm4.com
 *
 *  @see        http://libcsv.sourceforge.net/
 */
class CsvParser : public boost::noncopyable
{
public:

    /**
     * Construct a CsvParser object.
     * @param fp the open input file to parse
     */
    explicit CsvParser(FILE *fp = 0);

    ~CsvParser();

    /// Set file pointer.
    CsvParser& setFilePtr(FILE* fp);

    /// Set the field delimiter.
    CsvParser& setDelim(char delim);

    /// Set the quote character.
    CsvParser& setQuote(char quote);

    /// Set CSV_STRICT mode.  NOT recommended.  Default is false (lax).
    CsvParser& setStrict(bool enable);

    /// Set logging object.
    CsvParser& setLogger(log4cxx::LoggerPtr);

    /**
     * Return values for #getField .  Positive values indicate a libcsv
     * parsing error as returned by the csv_error() function---use
     * csv_strerror() to obtain a static string describing the error.
     */
    enum {
        OK = 0,                 ///< Returning a valid field
        END_OF_RECORD = -1,     ///< Reached end-of-record
        END_OF_FILE = -2,       ///< Reached end-of-file
        START_OF_FILE = -3      ///< Special internal value, never returned
    };

    /**
     * Return next field, END_OF_RECORD, or END_OF_FILE.
     *
     * @description Read a field, record terminator, or EOF from the
     * input file.  END_OF_RECORD is always returned separately,
     * i.e. you never get back both a field and an END_OF_RECORD, but
     * one or the other.  After the final END_OF_RECORD, END_OF_FILE
     * is returned for all subsequent calls.
     *
     * @c getField() can also return positive error values, which are
     * those returned by @c csv_error().  This typically only happens in
     * strict mode (and therefore #setStrict() is not recommended).
     * If a csv_error() value is returned, the parser is left in an
     * undefined state and further calls continue to return the error.
     *
     * @note The returned @c field pointer will never be NULL, so it
     * can safely be passed to the @c std::string constructor.
     *
     * @param field [OUT] non-NULL pointer to const parsed field or to empty string
     * @returns OK, END_OF_RECORD, END_OF_FILE, or csv_error() value
     */
    int getField(char const*& field);

    /**
     * Return true iff no fields are currently buffered up.
     */
    bool empty() const { return _fields.empty(); }

    /**
     * These accessors are for logging should an error occur.
     * @{
     */
    size_t getRecordNumber() const { return _lastField.record; }
    size_t getFieldNumber() const { return _lastField.column; }
    off_t  getFileOffset() const;
    std::string getLastField() const;
    /** @} */

private:
    int  more();                    // Parse another buffer's worth
    void putField(void*, size_t);   // Per-field logic
    void putRecord(int);            // Per-record logic

    // Callback routines needed by libcsv.
    static void fieldCbk(void* s, size_t n, void* state);
    static void recordCbk(int endChar, void* state);
    static int  spaceFunc(unsigned char ch);

    enum {
        // Miscellaneous constants
        BUF_SIZE = 8 * KiB,
        MAX_WARNINGS = 8
    };

    struct Field {
        int     offset;         // Offset of field data, or END_OF_* value
        size_t  record;         // Belongs to this record number
        size_t  column;         // Column within record
        off_t   filepos;        // Offset of field within file

        Field(int o, size_t r, size_t c, off_t f)
            : offset(o), record(r), column(c), filepos(f)
        {}
    };

    FILE*               _fp;        // Open file to parse
    int                 _csverr;    // Latched csv_error() value
    char                _delim;     // Field delimiter, 0 if unset
    char                _quote;     // Quote mark, 0 if unset
    csv_parser          _parser;    // Library parser state
    Field               _lastField; // Last real field (not END_OF_* value)
    std::deque<Field>   _fields;    // Queue of info about parsed fields
    std::vector<char>   _inbuf;     // File input buffer
    std::vector<char>   _data;      // Buffer holding parsed field data
    size_t              _datalen;   // Length of valid _data[]
    size_t              _numRecords;// Count records seen
    size_t              _numFields; // Track # of fields per record
    size_t              _prevFields;// Previous per-record field count
    size_t              _warnings;  // Field count warnings so far
    off_t               _fileOffset;// Input file offset prior to csv_parse call
    log4cxx::LoggerPtr  _logger;    // Logging support
};

} // namespace

#endif  /* ! CSV_PARSER_H_ */
