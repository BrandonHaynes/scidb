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
 * @file tsvparse.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief A tab-separated-value (TSV) parser.
 * @see http://dataprotocols.org/linear-tsv/
 */

#include <util/Utility.h>

namespace scidb {

bool tsv_parse(char *line, std::vector<char*>& fields, char delim)
{
    char *rp, *wp;              // read and write pointers
    rp = wp = line;
    char last = '\0';
    fields.clear();
    fields.push_back(line);

    for (rp = wp = line; *rp; ++rp, ++wp) {

        // Maybe starting a new field?
        if (last == delim)
            fields.push_back(wp);
        last = *rp;

        if (*rp == delim) {
            // Field separator, blow it away.
            *wp = '\0';
        }
        else if (*rp == '\n' || *rp == '\r') {
            // Record separator, blow it away and we are done.
            *wp = '\0';
            break;
        }
        else if (*rp == '\\') {
            // Escaped character, unescape it and bump rp.
            char ch = *++rp;
            switch (ch) {
            case 'n':
                *wp = '\n';
                break;
            case 'r':
                *wp = '\r';
                break;
            case 't':
                *wp = '\t';
                break;
            case '\\':
                *wp = '\\';
                break;
            case '\t':
                // Backslash at end-of-field is an error according to the spec.
                return false;
            default:
                // Unrecognized, pass along unmodified.
                *wp++ = '\\';
                *wp = ch;
                break;
            }
        }
        else {
            // Nothing special, copy it to output position.
            *wp = *rp;
        }
    }

    // Handle possible tab just prior to EOF (i.e. line not terminated
    // by record separator).
    if (last == '\t')
        fields.push_back(wp);

    // Since all line buffer changes are "shrinking", assert we didn't
    // overrun the line buffer.
    assert(wp <= rp);

    return true;
}

} // namespace


#ifdef TEST_PROGRAM

#include <iostream>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

int main(int, char**)
{
    size_t buflen = 128;
    char *bufp = static_cast<char*>(::malloc(buflen));
    vector<char *> fields;
    unsigned lineno = 0;

    while (EOF != ::getline(&bufp, &buflen, stdin)) {
        ++lineno;
        if (!scidb::tsv_parse(bufp, fields)) {
            cerr << "TSV parse error at line " << lineno
                 << ", probably caused by \\<TAB>" << endl;
            continue;
        }
        cout << "---- " << fields.size() << " fields ----" << endl;
        for (unsigned i = 0; i < fields.size(); ++i) {
            cout << '[' << fields[i] << ']' << endl;
        }
    }
}

#endif // TEST_PROGRAM
