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
 * @file TsvParser.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief A tab-separated-value (TSV) parser.
 * @see http://dataprotocols.org/linear-tsv/
 */

#include <util/TsvParser.h>
#include <util/Utility.h>       // for tsv_parse decl
#include <cassert>

namespace scidb {

TsvParser::TsvParser()
    : _cursor(0)
    , _eol(false)
    , _delim('\t')
{ }

TsvParser::TsvParser(char *line)
    : _cursor(line)
    , _eol(false)
    , _delim('\t')
{ }

void TsvParser::reset(char *line)
{
    _cursor = line;
    _eol = false;
}

static inline bool iseol(char ch)
{
    return ch == '\n' || ch == '\0' || ch == '\r';
}

void TsvParser::setDelim(char delim)
{
    // Choose any of these and you are in a world of hurt.
    assert(delim != '\n');
    assert(delim != '\r');
    assert(delim != '\\');
    assert(delim != '\0');

    _delim = delim;
}

int TsvParser::getField(char const*& field)
{
    assert(_cursor);
    if (_eol) {
        // We stay at EOL until somebody calls reset().  The _cursor
        // was left looking at the end-of-line character, so returning
        // it makes the TsvChunkLoader's _errorOffset calculation more
        // accurate, should it be needed.
        field = _cursor;
        return EOL;
    }

    char *rp, *wp;              // read and write pointers

    // Everything starts at the current _cursor.
    // rp: char we are considering
    // wp: char we're about to write (can be the same)
    // field: save initial cursor for return to caller
    field = rp = wp = _cursor;

    // We're going to edit the line buffer in-place as we go.
    // Therefore, we can never make a change that would increase the
    // overall buffer length.  In other words, for all chars x we assume
    // that the escape sequence \x never expands to more than bytes.

    for (;;) {
        if (*rp == _delim) {
            // End of field.
            *wp = '\0';
            _cursor = ++rp; // next call looks at next field
            return OK;
        }
        if (iseol(*rp)) {
            // Also end of field, but remember EOL state for next call.
            *wp = '\0';
            _eol = true;
            _cursor = rp;       // keep looking at eol
            return OK;
        }
        if (*rp == '\\') {
            // Escaped character, unescape it.
            ++rp;
            char ch = *rp++;    // rp is now past escape sequence
            if (ch == _delim || (_eol = iseol(ch))) {
                // Backslash at end-of-field is an error according to
                // the spec, and that should be true for non-standard
                // delimiters as well.
                *wp++ = '\\';
                *wp = '\0';
                _cursor = rp;   // next call looks at next field
                return ERR;
            }
            switch (ch) {
            case 'n':
                *wp++ = '\n';
                break;
            case 'r':
                *wp++ = '\r';
                break;
            case 't':
                *wp++ = '\t';
                break;
            case '\\':
                *wp++ = '\\';
                break;
            default:
                // Unrecognized, pass along unmodified.
                *wp++ = '\\';
                *wp++ = ch;
                break;
            }
        }
        else {
            // Nothing special, copy it to output position.
            *wp++ = *rp++;
        }
    }
    /*NOTREACHED*/
}

// For backward compatibility.  Used by the tsv2scidb program.
bool tsv_parse(char *line, std::vector<char*>& fields, char delim)
{
    TsvParser tp(line);
    tp.setDelim(delim);
    fields.clear();
    char const *field = 0;
    int rc = 0;

    for (;;) {
        rc = tp.getField(field);
        if (rc == TsvParser::EOL) {
            return true;
        }
        fields.push_back(const_cast<char*>(field));
        if (rc == TsvParser::ERR) {
            return false;
        }
    }
    /*NOTREACHED*/
}

} // namespace
