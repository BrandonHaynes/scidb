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

/**
 * @file CsvParser.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

// Module header always comes first.
#include <util/CsvParser.h>

#include <system/Utils.h>
#include <util/Platform.h>

#include <iostream>
#include <stdint.h>
#include <string.h>

#define LOG_WARNING(_x)                         \
do {                                            \
    if (_logger) {                              \
        LOG4CXX_WARN(_logger, _x);              \
    } else {                                    \
        std::cerr << _x << std::endl;           \
    }                                           \
} while (0)

using namespace std;

namespace scidb {

CsvParser::CsvParser(FILE *fp)
    : _fp(fp)
    , _csverr(0)
    , _delim('\0')
    , _quote('\0')
    , _lastField(START_OF_FILE, 0, 0, 0)
    , _inbuf(BUF_SIZE)
    , _data(BUF_SIZE + ::getpagesize())
    , _datalen(0)
    , _numRecords(0)
    , _numFields(0)
    , _prevFields(SIZE_MAX)
    , _warnings(0)
    , _fileOffset(0)
{
    int rc = csv_init(&_parser, 0);
    SCIDB_ASSERT(rc == 0);
    csv_set_space_func(&_parser, spaceFunc);

    // Empty _fields queue says: read more data!
    SCIDB_ASSERT(_fields.empty());
}

CsvParser::~CsvParser()
{
    csv_free(&_parser);
}

CsvParser& CsvParser::setFilePtr(FILE* fp)
{
    SCIDB_ASSERT(_fp == 0);
    SCIDB_ASSERT(fp != 0);
    _fp = fp;
    return *this;
}

CsvParser& CsvParser::setDelim(char delim)
{
    _delim = delim;
    csv_set_delim(&_parser, delim);
    return *this;
}

CsvParser& CsvParser::setQuote(char quote)
{
    _quote = quote;
    csv_set_quote(&_parser, quote);
    return *this;
}

CsvParser& CsvParser::setStrict(bool enable)
{
    // The problem with setting CSV_STRICT mode is that it's unclear
    // how to recover from an error and continue parsing.  This method
    // is here only so that we can experiment with it in the future.
    // For now, be mellow, dude!

    int opts = csv_get_opts(&_parser);
    if (enable) {
        opts |= CSV_STRICT;
    } else {
        opts &= ~CSV_STRICT;
    }
    csv_set_opts(&_parser, opts);
    return *this;
}

CsvParser& CsvParser::setLogger(log4cxx::LoggerPtr logger)
{
    _logger = logger;
    return *this;
}

int CsvParser::getField(char const*& field)
{
    SCIDB_ASSERT(_fp != 0);
    field = "";                  // Never return junk or NULL.

    if (_csverr) {
        // Errors are forever, at least until I figure out how to
        // recover from CSV_EPARSE when in strict mode.
        return _csverr;
    }

    // Parse next batch if we don't have anything.
    if (_fields.empty()) {
        _csverr = more();
        if (_csverr) {
            return _csverr;
        }
    }
    SCIDB_ASSERT(!_fields.empty());

    // Process head of queue.
    _lastField = _fields.front();
    _fields.pop_front();
    int offset = _lastField.offset;

    switch (offset) {
    case END_OF_RECORD:
        return END_OF_RECORD;
    case END_OF_FILE:
        SCIDB_ASSERT(_fields.empty());
        _fields.push_back(_lastField); // Once done, stay done.
        return END_OF_FILE;
    default:
        field = &_data[offset];
        return OK;
    }

    SCIDB_UNREACHABLE();
}

int CsvParser::more()
{
    SCIDB_ASSERT(_fields.empty()); // else why are we being called?

    _datalen = 0;           // Start reusing the _data buffer.

    // Read next input buffer.
    size_t nread = ::fread(&_inbuf[0], 1, BUF_SIZE, _fp);
    if (nread <= 0) {
        // End of file, finish up.
        csv_fini(&_parser, fieldCbk, recordCbk, this);
        _fields.push_back(Field(END_OF_FILE, _numRecords, _numFields, _fileOffset));
        return 0;
    }

    // Parse the buffer.
    size_t nparse =
        csv_parse(&_parser, &_inbuf[0], nread, fieldCbk, recordCbk, this);
    if (nparse != nread) {
        _fileOffset += nread;
        return csv_error(&_parser);
    }

    // Update _fileOffset *after* parsing is done, so that _fileOffset
    // and _datalen can be used to approximate the actual file offset
    // of a field.
    _fileOffset += nread;

    // If we had a good parse, we should have seen some fields.
    SCIDB_ASSERT(!_fields.empty());

    return 0;
}

void CsvParser::fieldCbk(void* s, size_t n, void* state)
{
    CsvParser* self = static_cast<CsvParser*>(state);
    self->putField(s, n);
}

void CsvParser::putField(void* s, size_t n)
{
    _numFields++;
    if ((_datalen + n + 1) >= _data.size()) {
        _data.resize(_data.size() + ::getpagesize());
    }
    _fields.push_back(Field(_datalen, _numRecords, _numFields, _fileOffset + _datalen));
    ::memcpy(&_data[_datalen], s, n);
    _datalen += n;
    _data[_datalen++] = '\0';
}

void CsvParser::recordCbk(int endChar, void* state)
{
    CsvParser* self = static_cast<CsvParser*>(state);
    self->putRecord(endChar);
}

void CsvParser::putRecord(int endChar)
{
    _numRecords++;

    if (endChar == -1) {
        // We are being called from csv_fini().
        LOG_WARNING("Last record (number " << _numRecords
                    << ") is missing a newline.");
    }

    _fields.push_back(Field(END_OF_RECORD, _numRecords, _numFields, _fileOffset + _datalen));
    size_t fieldCount = _numFields;
    _numFields = 0;

    // Changing field count is worth a warning... but not too many.
    if (_prevFields == SIZE_MAX) {
        _prevFields = fieldCount;
    } else if (_warnings < MAX_WARNINGS) {
        if (fieldCount != _prevFields) {
            LOG_WARNING("Field count changed from " << _prevFields << " to "
                        << fieldCount << " at input record " << _numRecords
                        << (++_warnings == MAX_WARNINGS ? " (Done complaining about this!)" : ""));
            _prevFields = fieldCount;
        }
    }
}

/**
 * Tell the libcsv parser what constitutes a space character.
 *
 * @description The parser ordinarily removes spaces and tabs from the
 * beginning and end of unquoted fields.  Apparently this is
 * undesirable, so we supply this callback to indicate that unquoted
 * fields should be left as is.
 *
 * @see Ticket #4353.
 */
int CsvParser::spaceFunc(unsigned char ch)
{
    return 0;
}

off_t CsvParser::getFileOffset() const
{
    // Sum of stuff prior to current parse plus latest field offset.
    // Because the fields are returned with trailing nulls, this will
    // include the approximate number of field delimiters.

    return _lastField.filepos;
}

string CsvParser::getLastField() const
{
    switch (_lastField.offset) {
    case START_OF_FILE:
        return "(unknown)";
    case END_OF_RECORD:
        return "(end-of-record)";
    case END_OF_FILE:
        return "(end-of-file)";
    default:
        SCIDB_ASSERT(_lastField.offset >= 0);
        return &_data[_lastField.offset];
    }
    SCIDB_UNREACHABLE();
}

} // namespace
